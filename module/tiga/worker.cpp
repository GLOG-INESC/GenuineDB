#include "module/tiga/worker.h"

#include <glog/logging.h>

#include <thread>

#include "common/proto_utils.h"

using std::make_pair;

namespace tiga {

using slog::kServerChannel;
using slog::kWorkerChannel;
using slog::MachineId;
using slog::MakeMachineId;
using slog::Metadata;
using slog::Record;
using slog::Sharder;
using slog::TransactionEvent;
using slog::TransactionStatus;
using slog::UnpackMachineId;
using slog::internal::Envelope;
using slog::internal::Request;
using slog::internal::Response;
using std::make_unique;

namespace {
uint64_t MakeTag(const RunId& run_id) { return run_id.first * 10 + run_id.second; }

inline std::ostream& operator<<(std::ostream& os, const RunId& run_id) {
  os << "(" << slog::GetTxnIdStr(run_id.first) << ", " << run_id.second << ")";
  return os;
}
}  // namespace

Worker::Worker(int id, const std::shared_ptr<Broker>& broker, const std::shared_ptr<SpeculativeMemOnlyStorage>& storage,
               const MetricsRepositoryManagerPtr& metrics_manager, std::chrono::milliseconds poll_timeout)
    : NetworkedModule(broker, kWorkerChannel + id, metrics_manager, poll_timeout),
      id_(id),
      storage_(storage),
      sharder_(slog::Sharder::MakeSharder(config())) {

  std::unique_ptr<Execution> execution;

  switch (config()->execution_type()) {
    case slog::internal::ExecutionType::KEY_VALUE:
      execution = make_unique<slog::KeyValueExecution>();
      break;
    case slog::internal::ExecutionType::TPC_C:
      execution = make_unique<slog::TPCCExecution>();
      break;
    default:
      execution = make_unique<slog::NoopExecution>();
      break;
  }

  execution_ = std::make_unique<slog::SpeculativeStorageExecution>(sharder_, config()->local_region(), std::move(execution), storage);
}

void Worker::Initialize() {
  zmq::socket_t sched_socket(*context(), ZMQ_PAIR);
  sched_socket.set(zmq::sockopt::rcvhwm, 0);
  sched_socket.set(zmq::sockopt::sndhwm, 0);
  sched_socket.connect(kSchedWorkerAddress + std::to_string(id_));

  AddCustomSocket(std::move(sched_socket));
}

void Worker::OnInternalRequestReceived(EnvelopePtr&& env) {
  CHECK_EQ(env->request().type_case(), Request::kRemoteReadResult) << "Invalid request for worker";
  auto& read_result = env->request().remote_read_result();
  auto run_id = make_pair(read_result.txn_id(), read_result.deadlocked());
  auto state_it = txn_states_.find(run_id);
  if (state_it == txn_states_.end()) {
    LOG(WARNING) << "Transaction " << run_id << " does not exist for remote read result";
    return;
  }

  VLOG(3) << "Got remote read result for txn " << run_id;

  auto& state = state_it->second;

  if (read_result.deadlocked()) {
    RECORD(state.txn->mutable_internal(), TransactionEvent::GOT_REMOTE_READS_DEADLOCKED);
  } else {
    RECORD(state.txn->mutable_internal(), TransactionEvent::GOT_REMOTE_READS);
  }

  if (state.txn->status() != TransactionStatus::ABORTED) {
    if (read_result.will_abort()) {
      // TODO: optimize by returning an aborting transaction to the scheduler immediately.
      // later remote reads will need to be garbage collected.
      state.txn->set_status(TransactionStatus::ABORTED);
      state.txn->set_abort_code(read_result.abort_code());
      state.txn->set_abort_reason(read_result.abort_reason());
    } else {
      // Apply remote reads.
      for (const auto& kv : read_result.reads()) {
        state.txn->mutable_keys()->Add()->CopyFrom(kv);
      }
    }
  }

  state.remote_reads_waiting_on--;

  // Move the transaction to a new phase if all remote reads arrive
  if (state.remote_reads_waiting_on == 0) {
    if (state.phase == TransactionState::Phase::WAIT_REMOTE_READ) {
      state.phase = TransactionState::Phase::EXECUTE;

      // Remove the redirection at broker for this txn
      StopRedirection(run_id);

      VLOG(3) << "Execute txn " << run_id << " after receving all remote read results";
    } else {
      LOG(FATAL) << "Invalid phase";
    }
  }

  AdvanceTransaction(run_id);
}

bool Worker::OnCustomSocket() {
  auto& sched_socket = GetCustomSocket(0);

  zmq::message_t msg;
  if (!sched_socket.recv(msg, zmq::recv_flags::dontwait)) {
    return false;
  }

  auto [txn, timestamp, definitive, rollback_flag] = *msg.data<slog::TigaIssueTxnMessage>();

  auto txn_id = txn->internal().id();

  // Transaction must be rolled back
  if (rollback_flag) {

    auto old_run_id = std::make_pair(txn_id, false);

    // If the transaction is still present in the state, then it is waiting for remote reads and has not modified state
    // Otherwise, it must have already been executed and requires a rollback
    if (txn_states_.erase(old_run_id)) {
      StopRedirection(old_run_id);
    } else {
      execution_->Rollback(*txn);
    }

    // Inform scheduler of succesfull rollback to release txn
    zmq::message_t rollback_confirmation_msg(sizeof(slog::TigaTxnExecutionResponse));
    *msg.data<slog::TigaTxnExecutionResponse>() = std::make_pair(txn_id, true);
    GetCustomSocket(0).send(msg, zmq::send_flags::none);

    return true;

  }

  auto run_id = std::make_pair(txn_id, definitive);


  RECORD(txn->mutable_internal(), TransactionEvent::ENTER_WORKER);

  // Create a state for the new transaction
  auto [iter, ok] = txn_states_.try_emplace(run_id, txn, timestamp, !definitive);

  CHECK(ok) << "Transaction " << txn_id << " has already been dispatched to this worker";

  iter->second.phase = TransactionState::Phase::READ_LOCAL_STORAGE;

  VLOG(3) << "Initialized state for txn " << txn_id;

  AdvanceTransaction(run_id);

  return true;
}

void Worker::AdvanceTransaction(const RunId& run_id) {
  auto& state = TxnState(run_id);
  switch (state.phase) {
    case TransactionState::Phase::READ_LOCAL_STORAGE:
      ReadLocalStorage(run_id);
      [[fallthrough]];
    case TransactionState::Phase::WAIT_REMOTE_READ:
      if (state.phase == TransactionState::Phase::WAIT_REMOTE_READ) {
        // The only way to get out of this phase is through remote messages
        break;
      }
      [[fallthrough]];
    case TransactionState::Phase::EXECUTE:
      if (state.phase == TransactionState::Phase::EXECUTE) {
        Execute(run_id);
      }
      [[fallthrough]];
    case TransactionState::Phase::FINISH:
      Finish(run_id);
      // Never fallthrough after this point because Finish and PreAbort
      // has already destroyed the state object
      break;
  }
}

void Worker::ReadLocalStorage(const RunId& run_id) {
  auto& state = TxnState(run_id);

  if (state.txn->status() != TransactionStatus::ABORTED) {
    for (auto& kv : *(state.txn->mutable_keys())) {
      const auto& key = kv.key();
      if (sharder_->is_local_key(key)) {
        auto value = kv.mutable_value_entry();
        if (Record record; storage_->Read(key, record)) {
          value->set_value(record.to_string());
        }
      }
    }
  }

  VLOG(3) << "Broadcasting local reads to other partitions";
  BroadcastReads(run_id);

  // Set the number of remote reads that this partition needs to wait for
  state.remote_reads_waiting_on = 0;

  const auto& waiting_partitions = state.txn->internal().active_partitions();

  if (state.txn->mutable_internal()->is_one_shot_txn()){
    state.remote_reads_waiting_on = 0;
  } else {
    if (std::find(waiting_partitions.begin(), waiting_partitions.end(), config()->local_machine_id()) !=
        waiting_partitions.end()) {
      // Waiting partition needs remote reads from all partitions
      state.remote_reads_waiting_on = state.txn->internal().involved_partitions_size() - 1;
    }
  }

  if (state.remote_reads_waiting_on == 0) {
    VLOG(3) << "Execute txn " << run_id << " without remote reads";
    state.phase = TransactionState::Phase::EXECUTE;
  } else {
    // Establish a redirection at broker for this txn so that we can receive remote reads
    StartRedirection(run_id);

    VLOG(3) << "Defer executing txn " << run_id << " until having enough remote reads";
    state.phase = TransactionState::Phase::WAIT_REMOTE_READ;
  }
}

void Worker::Execute(const RunId& run_id) {
  auto& state = TxnState(run_id);
  auto speculative = state.speculative_exec;

  switch (state.txn->program_case()) {
    case Transaction::kCode: {
      if (state.txn->status() != TransactionStatus::ABORTED) {
        execution_->Execute(*state.txn, speculative);
      }
      if (state.txn->status() == TransactionStatus::ABORTED) {
        VLOG(3) << "Txn " << run_id << " aborted with reason: " << state.txn->abort_reason();
      } else {
        VLOG(3) << "Committed txn " << run_id;
      }
      break;
    }
    default:
      LOG(FATAL) << "Invalid procedure";
  }
  state.phase = TransactionState::Phase::FINISH;
}

void Worker::Finish(const RunId& run_id) {
  auto& state = TxnState(run_id);

  RECORD(state.txn->mutable_internal(), TransactionEvent::EXIT_WORKER);

  auto coordinator = state.txn->internal().coordinating_server();
  auto [coord_reg, coord_rep, _] = UnpackMachineId(coordinator);
  if (coord_rep == config()->local_replica()) {
    Envelope env;
    auto tiga_finished_sub_txn = env.mutable_request()->mutable_tiga_finished_subtxn();
    // Add executed timestamp
    tiga_finished_sub_txn->set_timestamp(state.timestamp);
    // Add transaction
    auto finished_sub_txn = tiga_finished_sub_txn->mutable_finished_subtxn();
    finished_sub_txn->set_partition(config()->local_partition());

    if (state.speculative_exec) {
      finished_sub_txn->mutable_txn()->CopyFrom(*state.txn);
    } else {
      finished_sub_txn->set_allocated_txn(state.txn);
    }

    Send(env, coordinator, kServerChannel);
  }

  // Notify the scheduler that we're done
  zmq::message_t msg(sizeof(slog::TigaTxnExecutionResponse));
  *msg.data<slog::TigaTxnExecutionResponse>() = std::make_pair(run_id.first, false);
  GetCustomSocket(0).send(msg, zmq::send_flags::none);

  // Done with this txn. Remove it from the state map
  txn_states_.erase(run_id);

  VLOG(3) << "Finished with txn " << run_id;
}

void Worker::BroadcastReads(const RunId& run_id) {
  auto& state = TxnState(run_id);
  auto& txn = state.txn;

  if (txn->mutable_internal()->is_one_shot_txn()){
    return;
  }

  const auto& waiting_partitions = txn->internal().involved_partitions();

  auto local_partition = config()->local_partition();

  std::vector<MachineId> destinations;
  for (auto p : waiting_partitions) {
    if (p != config()->local_machine_id()) {
      destinations.push_back(p);
    }
  }

  if (destinations.empty()) {
    return;
  }

  auto aborted = txn->status() == TransactionStatus::ABORTED;

  // Send abort result and local reads to all remote active partitions
  Envelope env;
  auto rrr = env.mutable_request()->mutable_remote_read_result();
  rrr->set_txn_id(run_id.first);

  // We reuse deadlocked field from Detock, but it represents speculative or definitive execution
  rrr->set_deadlocked(run_id.second);

  rrr->set_partition(config()->local_machine_id());
  rrr->set_will_abort(aborted);
  rrr->set_abort_code(txn->abort_code());
  rrr->set_abort_reason(txn->abort_reason());

  if (!aborted) {
    auto reads_to_be_sent = rrr->mutable_reads();
    for (const auto& kv : txn->keys()) {
      reads_to_be_sent->Add()->CopyFrom(kv);
    }
  }

  Send(env, destinations, MakeTag(run_id));
}

TransactionState& Worker::TxnState(const RunId& run_id) {
  auto state_it = txn_states_.find(run_id);
  CHECK(state_it != txn_states_.end());
  return state_it->second;
}

void Worker::StartRedirection(const RunId& run_id) {
  auto redirect_env = NewEnvelope();
  redirect_env->mutable_request()->mutable_broker_redirect()->set_tag(MakeTag(run_id));
  redirect_env->mutable_request()->mutable_broker_redirect()->set_channel(channel());
  Send(move(redirect_env), Broker::MakeChannel(config()->broker_ports_size() - 1));
}

void Worker::StopRedirection(const RunId&  run_id) {
  auto redirect_env = NewEnvelope();
  redirect_env->mutable_request()->mutable_broker_redirect()->set_tag(MakeTag(run_id));
  redirect_env->mutable_request()->mutable_broker_redirect()->set_stop(true);
  Send(move(redirect_env), Broker::MakeChannel(config()->broker_ports_size() - 1));
}

}  // namespace janus