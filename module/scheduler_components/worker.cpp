#include "module/scheduler_components/worker.h"

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
#include "module/scheduler_components/remaster_manager.h"
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY) */

#include <glog/logging.h>

#include <thread>

#include "common/proto_utils.h"

using std::make_pair;

namespace slog {

namespace {
uint64_t MakeTag(const RunId& run_id) { return run_id.first * 10 + run_id.second; }

inline std::ostream& operator<<(std::ostream& os, const RunId& run_id) {
  os << "(" << TXN_ID_STR(run_id.first) << ", " << run_id.second << ")";
  return os;
}
}  // namespace

using internal::Envelope;
using internal::Request;
using internal::Response;
using std::make_unique;

Worker::Worker(int id, const std::shared_ptr<Broker>& broker, const std::shared_ptr<Storage>& storage,
               const MetricsRepositoryManagerPtr& metrics_manager, std::chrono::milliseconds poll_timeout, string debug_tag)
    : NetworkedModule(broker, kWorkerChannel + id, metrics_manager, poll_timeout), id_(id), storage_(storage), debug_tag_(name() + debug_tag),
      sharder_(Sharder::MakeSharder(config())),
      partition_(config()->local_partition()),
      region_(config()->local_region()){
  worker_id_ = MakeClientId(config()->local_region(), config()->local_replica(), config()->local_partition(), id);

  std::unique_ptr<Execution> execution;
  switch (config()->execution_type()) {
    case internal::ExecutionType::KEY_VALUE:
      execution = make_unique<KeyValueExecution>();
      break;
    case internal::ExecutionType::TPC_C:
      execution = make_unique<TPCCExecution>();
      break;
    default:
      execution = make_unique<NoopExecution>();
      break;
  }

  execution_ = make_unique<MemOnlyStorageExecution>(Sharder::MakeSharder(config()), config()->local_region(), std::move(execution), storage);


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
    LOG(WARNING) << debug_tag_ << "Transaction " << run_id << " does not exist for remote read result";
    return;
  }

  VLOG(3) << debug_tag_ << "Got remote read result for txn " << run_id;

  auto& state = state_it->second;
  auto& txn = state.txn_holder->txn();

  if (read_result.deadlocked()) {
    RECORD(txn.mutable_internal(), TransactionEvent::GOT_REMOTE_READS_DEADLOCKED);
  } else {
    RECORD(txn.mutable_internal(), TransactionEvent::GOT_REMOTE_READS);
  }

  if (txn.status() != TransactionStatus::ABORTED) {
    if (read_result.will_abort()) {
      // TODO: optimize by returning an aborting transaction to the scheduler immediately.
      // later remote reads will need to be garbage collected.
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_code(read_result.abort_code());
      txn.set_abort_reason(read_result.abort_reason());
    } else {
      // Apply remote reads.
      for (const auto& kv : read_result.reads()) {
        txn.mutable_keys()->Add()->CopyFrom(kv);
      }
    }
  }

  state.remote_reads_waiting_on--;

  // Move the transaction to a new phase if all remote reads arrive
  if (state.remote_reads_waiting_on == 0) {
    // Given this was the last remote read we were waiting for, add the event to the transaction
    // AddRemoteTxnEvent(txn.mutable_internal(), TransactionEvent::)
    //AddRemoteTxnEvent(txn.mutable_internal(), TransactionEvent::BROADCAST_READ, read_result.issuing_time(), env->from());

    // Used for profiling
    txn.mutable_internal()->set_last_remote_read(env->from());

    if (state.phase == TransactionState::Phase::WAIT_REMOTE_READ) {
      state.phase = TransactionState::Phase::EXECUTE;

      // Remove the redirection at broker for this txn
      StopRedirection(run_id);

      VLOG(3) << debug_tag_ << "Execute txn " << run_id << " after receving all remote read results";
    } else {
      LOG(FATAL) << debug_tag_ << "Invalid phase";
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

  auto [txn_holder, deadlocked] = *msg.data<std::pair<TxnHolder*, bool>>();
  auto& txn = txn_holder->txn();
  auto run_id = std::make_pair(txn.internal().id(), deadlocked);
  if (deadlocked) {
    auto old_run_id = std::make_pair(txn.internal().id(), false);
    // Clean up any transaction state created before the deadlock was detected
    if (txn_states_.erase(old_run_id)) {
      StopRedirection(old_run_id);
    }
  }

  RECORD(txn.mutable_internal(), TransactionEvent::ENTER_WORKER);

  // Clean the txn according to the execution scheme

#if defined(PARTIAL_EXEC) && defined(LOCK_MANAGER_DDR)
  // On partial execution mode, the worker only executes operations regarding its key set
  auto* key_values = txn.mutable_keys();
  int keep = 0;
  for (int i = 0; i < key_values->size(); i++){
    auto key_value = key_values->Get(i);

    if (key_value.value_entry().metadata().master() == region_ && sharder_->compute_partition(key_value.key()) == partition_){
      // Rearrange such that the first keys are the ones participating
      if (keep < i) {
        key_values->SwapElements(i, keep);
      }
      keep++;
    }
  }
  if (keep != key_values->size()){
    key_values->DeleteSubrange(keep, key_values->size()-keep);
  }
#endif

  // Create a state for the new transaction
  // TODO: Clean up txns that got into a deadlock
  auto [iter, ok] = txn_states_.try_emplace(run_id, txn_holder);

  CHECK(ok) << debug_tag_ << "Transaction " << run_id << " has already been dispatched to this worker";

  iter->second.phase = TransactionState::Phase::READ_LOCAL_STORAGE;

  VLOG(3) << debug_tag_ << "Initialized state for txn " << run_id;
  VLOG(3) << debug_tag_ << "Txn  " << TXN_ID_STR(txn_holder->txn_id()) << " Partitions: " << txn_holder->txn().internal().involved_partitions_size();

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
  auto txn_holder = state.txn_holder;
  auto& txn = txn_holder->txn();

  if (txn.status() != TransactionStatus::ABORTED) {
#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
    switch (RemasterManager::CheckCounters(txn, false, storage_)) {
      case VerifyMasterResult::VALID: {
        break;
      }
      case VerifyMasterResult::ABORT: {
        txn.set_status(TransactionStatus::ABORTED);
        txn.set_abort_reason("outdated counter");
        break;
      }
      case VerifyMasterResult::WAITING: {
        LOG(FATAL) << "Transaction " << run_id << " was sent to worker with a high counter";
        break;
      }
      default:
        LOG(FATAL) << "Unrecognized check counter result";
        break;
    }
#endif

    // We don't need to check if keys are in partition here since the assumption is that
    // the out-of-partition keys have already been removed
    for (auto& kv : *(txn.mutable_keys())) {
      const auto& key = kv.key();
      auto value = kv.mutable_value_entry();
      if (Record record; storage_->Read(key, record)) {
        // Check whether the stored master metadata matches with the information
        // stored in the transaction
        if (value->metadata().master() != record.metadata().master) {
          txn.set_status(TransactionStatus::ABORTED);
          txn.set_abort_reason("outdated master");
          break;
        }
        value->set_value(record.to_string());
      } else if (txn.program_case() == Transaction::kRemaster) {
        txn.set_status(TransactionStatus::ABORTED);
        txn.set_abort_reason("remaster non-existent key " + key);
        break;
      }

      /*
      else {
        LOG(ERROR) << debug_tag_ << "Key " << key << " Does not exist in this partition";
      }
       */
    }
  }

  VLOG(3) << debug_tag_ << "Broadcasting local reads to other partitions";
  BroadcastReads(run_id);

  // Set the number of remote reads that this partition needs to wait for
  state.remote_reads_waiting_on = 0;

#ifdef LOCK_MANAGER_DDR_OPTIMISTIC
  // If DDR is used, all partitions have to wait
  const auto& waiting_partitions = txn.internal().involved_partitions();
#else
  const auto& waiting_partitions = txn.internal().active_partitions();
#endif

  // In one-shot transactions, one never waits for any read results
  if (txn.mutable_internal()->is_one_shot_txn()) {
#ifdef LOCK_MANAGER_DDR_OPTIMISTIC

#ifdef PARTIAL_REP
    state.remote_reads_waiting_on = waiting_partitions.size() -1;
#else
    // Remove its own partition
    // If full execution, then wait for a response from all partitions
    auto partitions = std::set<PartitionId>();

    for (auto partition : waiting_partitions) {
      partitions.insert(GET_PARTITION_ID(partition));
    }

    if (partitions.count(config()->local_partition())) {
      // Account for itself
      state.remote_reads_waiting_on = partitions.size() -1;
    }
#endif

#else
    state.remote_reads_waiting_on = 0;
#endif
  } else {
    // Partial Exec and Full execution must wait for different partitions

#ifdef PARTIAL_EXEC
    // In partial exec, one must wait for response from all participating partitions if it has a write
    if (std::find(waiting_partitions.begin(), waiting_partitions.end(), MakeMachineId(config()->local_region(), 0, config()->local_partition())) !=
        waiting_partitions.end()) {
      // Waiting partition needs remote reads from all partitions
      state.remote_reads_waiting_on = txn.internal().involved_partitions_size() - 1;
    }
#else
    // If full execution, then wait for a response from all partitions
    auto partitions = std::set<PartitionId>();

    for (auto partition : waiting_partitions) {
      partitions.insert(GET_PARTITION_ID(partition));
    }

    if (partitions.count(config()->local_partition())) {
      // Account for itself
      state.remote_reads_waiting_on = partitions.size() -1;
    }
#endif
  }


  if (state.remote_reads_waiting_on == 0) {

    VLOG(3) << debug_tag_ << "Execute txn " << run_id << " without remote reads";

    state.phase = TransactionState::Phase::EXECUTE;

    // For profiling purposes, means that it did not wait for any remote read
    txn.mutable_internal()->set_last_remote_read(config()->local_machine_id());
  } else {
    // Establish a redirection at broker for this txn so that we can receive remote reads
    StartRedirection(run_id);

    VLOG(3) << debug_tag_ << "Defer executing txn " << run_id << " until having enough remote reads (" << state.remote_reads_waiting_on << ")";

    state.phase = TransactionState::Phase::WAIT_REMOTE_READ;
  }
}

void Worker::Execute(const RunId& run_id) {
  auto& state = TxnState(run_id);
  auto& txn = state.txn_holder->txn();

  RECORD(txn.mutable_internal(), TransactionEvent::EXECUTE_TXN);

  switch (txn.program_case()) {
    case Transaction::kCode: {
      if (txn.status() != TransactionStatus::ABORTED) {
        execution_->Execute(txn);
      }

      if (txn.status() == TransactionStatus::ABORTED) {
        VLOG(3) << debug_tag_ << "Txn " << run_id << " aborted with reason: " << txn.abort_reason();
      } else {
        VLOG(3) << debug_tag_ << "Committed txn " << run_id;
      }
      break;
    }
    case Transaction::kRemaster: {
      txn.set_status(TransactionStatus::COMMITTED);
      auto it = txn.keys().begin();
      const auto& key = it->key();
      Record record;
      storage_->Read(key, record);
      auto new_counter = it->value_entry().metadata().counter() + 1;
      record.SetMetadata(Metadata(txn.remaster().new_master(), new_counter));
      storage_->Write(key, record);

      state.txn_holder->SetRemasterResult(key, new_counter);
      break;
    }
    default:
      LOG(FATAL) << debug_tag_ << "Procedure is not set";
  }
  state.phase = TransactionState::Phase::FINISH;
}

void Worker::Finish(const RunId& run_id) {
  auto& state = TxnState(run_id);
  auto txn = state.txn_holder->FinalizeAndRelease();
  VLOG(3) << debug_tag_ << "Finalized txn " << run_id;

  RECORD(txn->mutable_internal(), TransactionEvent::EXIT_WORKER);

  // Send the txn back to the coordinating server if it is in the same replica.
  // This must happen before the sending to scheduler below. Otherwise,
  // the scheduler may destroy the transaction holder before we can
  // send the transaction to the server.
  auto coordinator = txn->internal().coordinating_server();
  auto [coord_reg, coord_rep, _] = UnpackMachineId(coordinator);

  // Split logic between Ziplog and Detock/SLOG
#ifdef FULL_EXEC
    if (coord_reg == config()->local_region()) {
#endif
      if (coord_rep == config()->local_replica()){
        Envelope env;
        auto finished_sub_txn = env.mutable_request()->mutable_finished_subtxn();
        finished_sub_txn->set_partition(config()->local_partition());
        finished_sub_txn->set_allocated_txn(txn);
        VLOG(3) << debug_tag_ << "Sent txn " << run_id << " to server " << MACHINE_ID_STR(txn->internal().coordinating_server());

        Send(env, txn->internal().coordinating_server(), kServerChannel);
      } else {
        delete txn;
      }
#ifdef FULL_EXEC
    } else {
      delete txn;
    }
#endif

  // Notify the scheduler that we're done
  zmq::message_t msg(sizeof(TxnId));
  *msg.data<TxnId>() = run_id.first;
  GetCustomSocket(0).send(msg, zmq::send_flags::none);

  // Done with this txn. Remove it from the state map
  txn_states_.erase(run_id);

  VLOG(3) << debug_tag_ << "Finished with txn " << run_id;
}

void Worker::BroadcastReads(const RunId& run_id) {
  auto& state = TxnState(run_id);
  auto txn_holder = state.txn_holder;
  auto& txn = txn_holder->txn();


#ifdef LOCK_MANAGER_DDR_OPTIMISTIC
  // If DDR is used, all partitions have to wait
  const auto& waiting_partitions = txn.internal().involved_partitions();
#else
  // GLOG does not require broadcasting for one-shot transactions
  if (txn.mutable_internal()->is_one_shot_txn()){
    return;
  }
  const auto& waiting_partitions = txn.internal().active_partitions();
#endif


  auto local_region = config()->local_region();
  auto local_replica = config()->local_replica();
  auto local_partition = config()->local_partition();

  std::vector<MachineId> destinations;

  // In Partial execution, we must broadcast the result to ALL partitions

  auto partitions = std::set<PartitionId>();

  for (const MachineId& p_id : waiting_partitions) {
#ifdef FULL_EXEC
    if (GET_PARTITION_ID(p_id) != local_partition && partitions.count(GET_PARTITION_ID(p_id)) == 0){
      partitions.insert(GET_PARTITION_ID(p_id));
      destinations.push_back(MakeMachineId(local_region, local_replica, GET_PARTITION_ID(p_id)));
      VLOG(2) << debug_tag_ << "Issuing Remote read to " << MACHINE_ID_STR(MakeMachineId(local_region, local_replica, GET_PARTITION_ID(p_id)));
    }
#elif PARTIAL_EXEC
    // One shot transactions still need to exchange information between regions
    if (p_id != config()->local_machine_id()){
      destinations.push_back(p_id);
      VLOG(2) << debug_tag_ << "Issuing Remote read to " << MACHINE_ID_STR(p_id);
    }
#endif
  }

  if (destinations.empty()) {
    return;
  }

  auto aborted = txn.status() == TransactionStatus::ABORTED;

  // Send abort result and local reads to all remote active partitions
  Envelope env;
  auto rrr = env.mutable_request()->mutable_remote_read_result();
  rrr->set_txn_id(run_id.first);
  rrr->set_deadlocked(run_id.second);
  rrr->set_partition(MakeMachineId(local_region, local_replica, local_partition));
  rrr->set_will_abort(aborted);
  rrr->set_abort_code(txn.abort_code());
  rrr->set_abort_reason(txn.abort_reason());
  rrr->set_issuing_time(std::chrono::system_clock::now().time_since_epoch().count());

  // One-shot transactions only issue a message signifying the existence of deadlocks, nothing else
  if (!aborted && !txn.mutable_internal()->is_one_shot_txn()) {
    auto reads_to_be_sent = rrr->mutable_reads();
    for (const auto& kv : txn.keys()) {
      reads_to_be_sent->Add()->CopyFrom(kv);
    }
  }

  RECORD(txn.mutable_internal(), TransactionEvent::BROADCAST_READ);
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
  // Record the log for debugging
  if (per_thread_metrics_repo != nullptr){
    if (config()->metric_options().active_redirect_sample()){
      per_thread_metrics_repo->RecordActiveRedirect(worker_id_,
                                                    std::chrono::system_clock::now().time_since_epoch().count(),
                                                    active_redirects_++);
    }
  }

}

void Worker::StopRedirection(const RunId& run_id) {
  auto redirect_env = NewEnvelope();
  redirect_env->mutable_request()->mutable_broker_redirect()->set_tag(MakeTag(run_id));
  redirect_env->mutable_request()->mutable_broker_redirect()->set_stop(true);
  Send(move(redirect_env), Broker::MakeChannel(config()->broker_ports_size() - 1));
  if (per_thread_metrics_repo != nullptr){
    if (config()->metric_options().active_redirect_sample()){
      per_thread_metrics_repo->RecordActiveRedirect(worker_id_,
                                                    std::chrono::system_clock::now().time_since_epoch().count(),
                                                    active_redirects_--);
    }
  }
}

}  // namespace slog