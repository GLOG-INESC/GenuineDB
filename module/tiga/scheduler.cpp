#include "module/tiga/scheduler.h"

#include <algorithm>
#include <iostream>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

#include "common/bitmap.h"
#include "common/clock.h"
#include "common/json_utils.h"
#include "common/metrics.h"
#include "common/proto_utils.h"
#include "common/types.h"
#include "proto/internal.pb.h"

namespace tiga {

using std::make_shared;
using std::move;
using std::shared_ptr;
using std::unordered_set;
using std::vector;
using std::chrono::milliseconds;

using slog::kMachineIdBits;
using slog::kPartitionIdBits;
using slog::kRegionIdBits;
using slog::kReplicaIdBits;
using slog::kSchedulerChannel;
using slog::MachineId;
using slog::MakeMachineId;
using slog::MakeRunnerFor;
using slog::per_thread_metrics_repo;
using slog::internal::Request;
using slog::internal::Response;
using slog::TransactionEvent;

Scheduler::Scheduler(const shared_ptr<Broker>& broker, const shared_ptr<SpeculativeMemOnlyStorage>& storage,
                     const MetricsRepositoryManagerPtr& metrics_manager, std::chrono::milliseconds poll_timeout,
                     bool debug_mode)
    : NetworkedModule(broker, {kSchedulerChannel, false /* is_raw */}, metrics_manager, poll_timeout),
      current_worker_(0),
      debug_mode_(debug_mode) {
  std::stringstream scheduler_id_debug;
  scheduler_id_debug << "[SCHEDULER_" << (int)config()->local_region() << "_" << (int)config()->local_replica() << "_"
                     << (int)config()->local_partition() << "] ";
  scheduler_id_debug_ = scheduler_id_debug.str();

  for (int i = 0; i < config()->num_workers(); i++) {
    workers_.push_back(MakeRunnerFor<Worker>(i, broker, storage, metrics_manager, poll_timeout));
  }

  execSequencer_.reserve(125000000);
  entriesInSpec_.reserve(125000000);
}

void Scheduler::Initialize() {
  auto cpus = config()->cpu_pinnings(slog::ModuleId::WORKER);
  size_t i = 0;

  for (auto& worker : workers_) {
    // On debug mode, skip initialization of workers to test Scheduler in isolation
    if (!debug_mode_) {
      std::optional<uint32_t> cpu = {};
      if (i < cpus.size()) {
        cpu = cpus[i];
      }
      worker->StartInNewThread(cpu);
    }

    zmq::socket_t worker_socket(*context(), ZMQ_PAIR);
    worker_socket.set(zmq::sockopt::rcvhwm, 0);
    worker_socket.set(zmq::sockopt::sndhwm, 0);
    worker_socket.bind(kSchedWorkerAddress + std::to_string(i));

    AddCustomSocket(move(worker_socket));

    i++;
  }
}

void Scheduler::OnInternalRequestReceived(EnvelopePtr&& env) {
  switch (env->request().type_case()) {
    case Request::kSpecExecution:
      ProcessTransaction(move(env));
      break;
    case Request::kSpecConfirmation:
      ProcessAgreementConfirmation(move(env));
      break;
    case Request::kStats:
      PrintStats(move(env));
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(env->request().type_case(), Request) << "\"";
      break;
  }
}

void Scheduler::HandleWrongSpeculation(TigaTxnHolder* txn_holder, std::vector<TxnId>& candidate_txns,
                                       uint64_t definitive_ts) {
  // If the transaction was already issued for execution, then must roll back its previous exec
      std::stringstream rollback_txn;
  rollback_txn << " Rollback Transaction " << txn_holder->txn_id_;

  if (txn_holder->issued_for_execution_) {

    // Set flag to ignore first result (i.e. speculative execution) of this transaction
    if (txn_holder->completed_execution_) {
      rollback_txn << " Already finished execution";
      txn_holder->completed_execution_ = false;
    } else {
      rollback_txn << " Waiting for result";

      txn_holder->ignore_result = true;
    }

    VLOG(2) << rollback_txn.str();
    // Issue txn to be rolled back
    slog::TigaIssueTxnMessage data = std::make_tuple(txn_holder->txn_, txn_holder->spec_timestamp_, true, true);
    zmq::message_t msg(sizeof(data));
    *msg.data<decltype(data)>() = data;
    GetCustomSocket(txn_holder->worker_).send(msg, zmq::send_flags::none);

  } else {
    rollback_txn << " Transaction not issued, removing locks";
    std::unordered_set<TxnId> unique_candidates;
    VLOG(2) << rollback_txn.str();

    auto seq_entry = SeqEntry{txn_holder->spec_timestamp_, txn_holder->txn_id_, {}};
    // Otherwise, remove all locks requests
    for (auto const& [key, type] : txn_holder->keys_) {
      execSequencer_[key].erase(seq_entry);

      // If waiting transactions with lower timestamps exist, then they are candidates for execution
      if (!execSequencer_[key].empty() && execSequencer_[key].begin()->timestamp < definitive_ts &&
          unique_candidates.count(execSequencer_[key].begin()->txnId) == 0) {
        unique_candidates.insert(execSequencer_[key].begin()->txnId);
        candidate_txns.push_back(execSequencer_[key].begin()->txnId);
      }
    }
  }
}

void Scheduler::ProcessTransaction(EnvelopePtr&& env) {
  auto proposed_txn = env->mutable_request()->mutable_spec_execution();
  auto proposed_timestamp = proposed_txn->timestamp();
  auto spec_execution = proposed_txn->speculative();
  TxnId txn_id = 0;
  Transaction* txn;
  TigaTxnHolder* txn_holder;

  bool is_first_time = proposed_txn->has_txn();

  std::vector<TxnId> candidate_txns;

  // If the message has a transaction, it's the first time hearing about it
  if (is_first_time) {
    txn = proposed_txn->release_txn();
    txn_id = txn->internal().id();

    CHECK(txn_id != 0) << "Sanity check that txn_id is never 0";

    auto [it, inserted] = txns_.try_emplace(txn_id, nullptr);

    CHECK(inserted) << "TxnId already existed";
    RECORD(txn->mutable_internal(), TransactionEvent::ENTER_SCHEDULER);

    it->second = new TigaTxnHolder(txn, spec_execution);
    txn_holder = it->second;
  } else {
    // The transaction is being reissued due to wrong speculation
    // Check that it still exists and issue rollbacks if necessary
    txn_id = proposed_txn->txn_id();

    auto txn_holder_it = txns_.find(txn_id);
    CHECK(txn_holder_it != txns_.end()) << " Transaction no longer exists in scheduler";

    txn_holder = txn_holder_it->second;
    txn = txn_holder->txn_;

    // Handle wrongful speculation and no need to check for execution
    HandleWrongSpeculation(txn_holder, candidate_txns, proposed_timestamp);
  }

  // If the execution is speculative, then it has not yet reached agreement
  txn_holder->reached_agreement_ = !spec_execution;
  txn_holder->spec_timestamp_ = proposed_timestamp;

  // Check for Conflict Detection with previously released transactions
  for (auto const& [key, type] : txn_holder->keys_) {
    auto [set_it, insert_check] = execSequencer_[key].emplace(SeqEntry{proposed_timestamp, txn_id, type});
    CHECK(insert_check) << "Txn already existed";
  }

  CheckTxnForExecution(txn_holder);

  // Check all other candidate txns for execution as well
  for (auto& candidate_txn_id : candidate_txns) {
    auto candidate_txn_it = txns_.find(candidate_txn_id);

    CHECK(candidate_txn_it != txns_.end()) << "Candidate txn no longer exists";

    CheckTxnForExecution(candidate_txn_it->second);
  }
}

void Scheduler::ReleaseLocks(TigaTxnHolder* txn_holder) {
  std::vector<TxnId> candidate_txns;
  std::unordered_set<TxnId> unique_candidates;

  for (auto const& [key, _] : txn_holder->keys_) {
    bool lock_free = entriesInSpec_[key].ReleaseLock(txn_holder->txn_id_);

    if (lock_free && !execSequencer_[key].empty()) {
      for (auto& [timestamp, txn_id, type] : execSequencer_[key]) {
        if (unique_candidates.count(txn_id) == 0) {
          candidate_txns.push_back(txn_id);
          unique_candidates.insert(txn_id);
        }
        if (type == slog::WRITE) {
          break;
        }
      }
    }
  }

  for (auto& candidate_txn_id : candidate_txns) {
    auto candidate_txn_it = txns_.find(candidate_txn_id);

    CHECK(candidate_txn_it != txns_.end()) << "Candidate txn no longer exists";

    CheckTxnForExecution(candidate_txn_it->second);
  }
}

void Scheduler::ProcessAgreementConfirmation(EnvelopePtr&& env) {
  auto agreement_confirmation = env->mutable_request()->mutable_spec_confirmation();
  auto txn_id = agreement_confirmation->txn_id();
  auto confirmation = agreement_confirmation->spec_confirmation();

  auto it = txns_.find(txn_id);

  CHECK(it != txns_.end()) << "Process Confirmation should only really come after the txn";

  auto txn_holder = it->second;

  // Speculation was correct, mark agreement finish
  if (confirmation) {
    txn_holder->reached_agreement_ = true;
    VLOG(2) << scheduler_id_debug_ << "Got Timestamp Confirmation on Txn " << txn_id;

    // Txn has completed successfully as well, release locks and determine possible txns for execution
    if (txn_holder->completed_execution_) {
    VLOG(2) <<  scheduler_id_debug_ << "Txn " << txn_id << " complete, releasing locks";

      ReleaseLocks(txn_holder);
      // Transaction was issued speculatively, so we must handle the txn memory leftovers
      delete txn_holder->txn_;

      delete txn_holder;
      txns_.erase(txn_id);
    }
  }
}

void Scheduler::ProcessExecutionConfirmation(TxnId txn_id, bool is_rollback) {
  auto txn_holder_it = txns_.find(txn_id);
  CHECK(txn_holder_it != txns_.end()) << " Transaction no longer exists in scheduler";

  auto txn_holder = txn_holder_it->second;

  // Speculative execution was wrong, ignore this result
  // If its a rollback confirmation and ignore_result was set, means scheduler wont get the response either way
  if (is_rollback) {
    txn_holder->ignore_result = false;

    VLOG(2) << scheduler_id_debug_ << "Rolled Back Txn " << txn_id;
    ReleaseLocks(txn_holder);
    return;
  }

  if (txn_holder->ignore_result) {
    txn_holder->ignore_result = false;
    VLOG(2) <<  scheduler_id_debug_ << "Ignoring first result of " << txn_id;

  } else {
    VLOG(2) <<  scheduler_id_debug_ << "Got execution of " << txn_id;

    txn_holder->completed_execution_ = true;

    // If transaction has finished execution and reached agreement, clean state and issue the next transaction
    if (txn_holder->reached_agreement_) {
      VLOG(2) <<  scheduler_id_debug_ << "Txn " << txn_id << " complete, releasing locks";

      ReleaseLocks(txn_holder);
      delete txn_holder;
      txns_.erase(txn_id);
    }
  }
}

void Scheduler::CheckTxnForExecution(TigaTxnHolder* txn_holder) {
  bool can_exec = true;
  auto txn = txn_holder->txn_;
  // Check for Conflict Detection with previously released transactions
  for (auto const& [key, type] : txn_holder->keys_) {
    can_exec &=
        ((execSequencer_[key].begin()->txnId == txn->internal().id()) && (entriesInSpec_[key].CanAcquireLock(type)));
  }

  if (can_exec) {

    auto seq_entry = SeqEntry{txn_holder->spec_timestamp_, txn_holder->txn_id_, {}};
    for (auto const& [key, type] : txn_holder->keys_) {
      entriesInSpec_[key].AcquireLock(txn->internal().id(), type);
      execSequencer_[key].erase(seq_entry);
    }

    if (txn_holder->reached_agreement_) {
      RECORD(txn->mutable_internal(), TransactionEvent::DISPATCH_DEF);
    } else {
      RECORD(txn->mutable_internal(), TransactionEvent::DISPATCH_SPEC);
    }

    // Issue txn to be executed
    // Reached_agreement flag is used to determine whether its a speculative or a definitive execution
    slog::TigaIssueTxnMessage data =
        std::make_tuple(txn, txn_holder->spec_timestamp_, txn_holder->reached_agreement_, false);
    zmq::message_t msg(sizeof(data));
    *msg.data<decltype(data)>() = data;

    if (txn_holder->worker_ == -1) {
      txn_holder->worker_ = current_worker_;
      current_worker_ = (current_worker_ + 1) % workers_.size();
    }

    VLOG(2) << scheduler_id_debug_ << "Issuing Txn " << txn_holder->txn_id_ << " for execution";
    GetCustomSocket(txn_holder->worker_).send(msg, zmq::send_flags::none);

    txn_holder->issued_for_execution_ = true;
  }
}

void Scheduler::OnInternalResponseReceived(EnvelopePtr&& env) {}

// Handle responses from the workers
bool Scheduler::OnCustomSocket() {
  bool has_msg = false;
  bool stop = false;
  while (!stop) {
    stop = true;
    for (size_t i = 0; i < workers_.size(); i++) {
      if (zmq::message_t msg; GetCustomSocket(i).recv(msg, zmq::recv_flags::dontwait)) {
        stop = false;
        has_msg = true;

        auto [txn_id, is_rollback] = *msg.data<slog::TigaTxnExecutionResponse>();
        ProcessExecutionConfirmation(txn_id, is_rollback);
      }
    }
  };

  return has_msg;
}

using std::cout;

void Scheduler::PrintStats(EnvelopePtr&& env) { auto& stat_request = env->request().stats(); }

}  // namespace tiga