#include "module/sequencer.h"

#include <glog/logging.h>

#include <algorithm>

#include "common/clock.h"
#include "common/proto_utils.h"
#include "common/replication_utils.h"
#include "paxos/simulated_multi_paxos.h"
#include "common/memory_allocation_utils.h"
using std::move;

namespace slog {

using internal::Request;
using std::chrono::milliseconds;

/** Batch Replication */

BatchReplicationHandler::BatchReplicationHandler(slog::Batch* batch, int local_rep_factor, int global_rep_factor)
    : batch_(batch),
      local_rep_factor_(local_rep_factor),
      global_rep_factor_(global_rep_factor) {

}

BatchReplicationHandler::BatchReplicationHandler(int local_rep_factor, int global_rep_factor)
    : BatchReplicationHandler(nullptr, local_rep_factor, global_rep_factor) {}

void BatchReplicationHandler::AddAck(slog::RegionId reg) {
  if (++region_rep_factor[reg] == local_rep_factor_){
    global_rep_++;
  }
}

void BatchReplicationHandler::AddAck(Batch* batch, slog::RegionId reg) {
  // We should only add a batch if there is no assigned one yet
  CHECK(batch_ == nullptr);

  batch_ = batch;
  AddAck(reg);
}

bool BatchReplicationHandler::ReplicationComplete() {
  return global_rep_ >= global_rep_factor_ && batch_ != nullptr;
}

Batch* BatchReplicationHandler::GetBatch() {
  return batch_;
}

bool BatchReplicationHandler::IsInitialized() {
  return initialized_;
}

void BatchReplicationHandler::Initialize(int local_rep_factor, int global_rep_factor) {
  CHECK(!initialized_) << "Batch was already initialized";
  CHECK_EQ(local_rep_factor, 1) << "Currently not supporting replication";
  CHECK_EQ(global_rep_factor, 1) << "Currently not supporting replication";

  local_rep_factor_ = local_rep_factor;
  global_rep_factor_ = global_rep_factor;
  initialized_ = true;
}

/** Sequencer */

Sequencer::Sequencer(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
                     const MetricsRepositoryManagerPtr& metrics_manager, milliseconds poll_timeout)
    : NetworkedModule(context, config, config->sequencer_port(), kSequencerChannel, metrics_manager, poll_timeout),
      batcher_(std::make_shared<Batcher>(context, config, metrics_manager, poll_timeout)),
      batcher_runner_(std::static_pointer_cast<Module>(batcher_)) {

  // Preallocate very large batch handler
  batch_handler_.emplace_back(static_cast<BatchBlock*>(AllocateHugePage(sizeof(BatchBlock))));
  batch_block_ = batch_handler_.begin();
  current_batch_ = (*batch_block_)->begin();

  std::stringstream sequencer_debug;

  sequencer_debug << "[SEQUENCER_" << (int)config->local_region() << "_" << (int)config->local_replica() << "_" << config->local_partition() << "] ";
  sequencer_debug_ = sequencer_debug.str();
}

void Sequencer::Initialize() { batcher_runner_.StartInNewThread(); }

void Sequencer::OnInternalRequestReceived(EnvelopePtr&& env) {
  auto request = env->mutable_request();
  switch (request->type_case()) {
    case Request::kForwardTxn:
      ProcessForwardRequest(move(env));
      break;
    case Request::kGlogBatch:
      ProcessForwardBatchRequest(move(env));
      break;
    case Request::kPing:
      ProcessPingRequest(move(env));
      break;
    case Request::kStats:
      Send(move(env), kBatcherChannel);
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(request->type_case(), Request) << "\"";
      break;
  }
}

void Sequencer::ProcessForwardRequest(EnvelopePtr&& env) {
  auto now = std::chrono::high_resolution_clock::now().time_since_epoch().count();
  auto txn = env->mutable_request()->mutable_forward_txn()->mutable_txn();
  auto txn_internal = txn->mutable_internal();

  RECORD(txn_internal, TransactionEvent::ENTER_SEQUENCER);

  txn_internal->set_mh_arrive_at_home_time(now);

  if (config()->bypass_mh_orderer() && per_thread_metrics_repo != nullptr) {
    per_thread_metrics_repo->RecordTxnTimestamp(txn_internal->id(), env->from(), txn_internal->timestamp(), now);
  }

  if (config()->bypass_mh_orderer() && config()->synchronized_batching()) {
    if (txn_internal->timestamp() <= now) {
      VLOG(2) << sequencer_debug_ << "Txn " << TXN_ID_STR(txn_internal->id()) << " has a timestamp "
              << (now - txn_internal->timestamp()) / 1000 << " us in the past";

#if !defined(LOCK_MANAGER_DDR)
      // If not using DDR, restart the transaction
      txn->set_status(TransactionStatus::ABORTED);
      txn->set_abort_code(AbortCode::RESTARTED);
#endif
    } else {
      VLOG(2) << sequencer_debug_ << "Txn " << TXN_ID_STR(txn_internal->id()) << " has a timestamp "
              << (txn_internal->timestamp() - now) / 1000 << " us into the future";

      RECORD_WITH_TIME(txn_internal, TransactionEvent::EXPECTED_WAIT_TIME_UNTIL_ENTER_LOCAL_BATCH,
                       txn_internal->timestamp() - now);
    }
    // Put into a sorted buffer and wait until local clock reaches the txn's timestamp.
    // Send a signal to the batcher if the earliest time in the buffer has changed, so that
    // the batcher is rescheduled to wake up at this ealier time
    bool signal_needed = batcher_->BufferFutureTxn(env->mutable_request()->mutable_forward_txn()->release_txn());
    if (signal_needed) {
      auto env = NewEnvelope();
      env->mutable_request()->mutable_signal();
      Send(move(env), kBatcherChannel);
    }
  } else {
    // Put to batch immediately
    txn_internal->set_mh_enter_local_batch_time(now);
    Send(move(env), kBatcherChannel);
  }
}

void Sequencer::ProcessForwardBatchRequest(EnvelopePtr&& env) {
  //Send(move(env), kBatcherChannel);


  auto glog_batches = env->mutable_request()->mutable_glog_batch();

  for (int i = 0; i < glog_batches->batches_size(); i++){

    auto glog_batch = glog_batches->mutable_batches(i);
    // If the slot was already correctly replicated, do nothing
    if (glog_batch->slot() < current_slot_){
      // skip
      return;
    }

    // Get the originating region
    auto [from_region, from_replica, from_partition] = UnpackMachineId(env->from());

    // Get the right entry
    auto block = (glog_batch->slot() / slog::NUM_ENTRIES_PER_BLOCK) - processed_blocks_;

    while (allocated_blocks_-1 < block){
      batch_handler_.emplace_back(static_cast<BatchBlock*>(AllocateHugePage(sizeof(BatchBlock))));
      allocated_blocks_++;
    }

    auto block_it = batch_handler_.begin();
    std::advance(block_it, block);
    // Get the correct batch handler
    BatchReplicationHandler* batch_handler = &((*block_it)->data())[glog_batch->slot() % slog::NUM_ENTRIES_PER_BLOCK];

    if (!batch_handler->IsInitialized()){
      batch_handler->Initialize(more_than_half(config()->num_replicas(config()->local_region())),
                                (int)config()->replication_factor());
    }

    if (glog_batch->has_txns()) {
      auto batch = glog_batch->release_txns();
      for (auto txn_it = 0; txn_it < batch->transactions_size(); txn_it++){
        RECORD(batch->mutable_transactions(txn_it)->mutable_internal(), TransactionEvent::ENTER_SEQUENCER);
      }

      batch_handler->AddAck(batch, from_region);
    } else {
      batch_handler->AddAck(from_region);
    }

    if (current_batch_ != nullptr && current_batch_->ReplicationComplete()){
      auto new_env = NewEnvelope();
      auto new_glog_batches = new_env->mutable_request()->mutable_glog_batch();

      while (current_batch_ != nullptr && current_batch_->ReplicationComplete()){
        new_glog_batches->add_batches()->set_allocated_txns(current_batch_->GetBatch());

        // move batch
        auto next_block = ++current_slot_ / slog::NUM_ENTRIES_PER_BLOCK;

        // Finished processing current block, dealoc memory go next
        if (next_block > processed_blocks_){

          // Free memory
          processed_blocks_++;

          // Before deallocating, check if we need to first allocate a new block

          if (allocated_blocks_-1 == 0){
            batch_handler_.emplace_back(static_cast<BatchBlock*>(AllocateHugePage(sizeof(BatchBlock))));
          } else {
            allocated_blocks_--;
          }

          batch_block_++;

          // Now deallocate the memory
          auto* processed_block = batch_handler_.front();
          std::free(processed_block);
          batch_handler_.pop_front();
        }

        current_batch_ = &(*batch_block_)->operator[](current_slot_%slog::NUM_ENTRIES_PER_BLOCK);
      }

      Send(move(new_env), kBatcherChannel);

    }

  }


}

void Sequencer::ProcessPingRequest(EnvelopePtr&& env) {
  auto now = slog_clock::now().time_since_epoch().count();
  auto pong_env = NewEnvelope();
  auto pong = pong_env->mutable_response()->mutable_pong();
  pong->set_src_time(env->request().ping().src_time());
  pong->set_dst_time(now);
  pong->set_dst(env->request().ping().dst());
  Send(move(pong_env), env->from(), kForwarderChannel);
}

}  // namespace slog