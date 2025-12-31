#include <glog/logging.h>

#include "common/clock.h"
#include "common/constants.h"
#include "common/json_utils.h"
#include "common/log/multi_slot_log.h"
#include "common/log/deterministic_merge_log.h"
#include "common/log/pessimistic_dm_log.h"

#include "common/proto_utils.h"
#include "glog.h"
#include "proto/internal.pb.h"
using std::shared_ptr;

namespace glog {

using namespace slog;
using std::chrono::milliseconds;

using internal::Envelope;
using internal::Request;
using internal::Response;

GLOG::GLOG(int id, const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config, const MetricsRepositoryManagerPtr& metrics_manager, std::chrono::milliseconds poll_timeout)
    : NetworkedModule(context, config, config->glog_port(), kGLOGChannel, metrics_manager, poll_timeout, true /* is_long_sender */),
      id_(id){


  std::stringstream ziplog_manager_debug;

  ziplog_manager_debug << "[GLOG_" << (int)config->local_region() << "_" << (int)config->local_replica() << "_" << config->local_partition() << "_" << id_ << "] ";
  ziplog_manager_debug_ = ziplog_manager_debug.str();

  auto local_region = config->local_region();
  auto local_replica = config->local_replica();
  auto local_partition = config->local_partition();

  for (RegionId r = 0; r < config->num_regions(); r++){
    if (static_cast<RegionId>(r) != local_region) {
      other_regions_.push_back(MakeMachineId(r, 0, config->leader_partition_for_multi_home_ordering()));
    }
  }

  for (int r = 0; r < config->num_replicas(local_region); r++) {
    if (static_cast<ReplicaId>(r) != local_replica) {
      other_replicas_.push_back(MakeMachineId(local_region, r, local_partition));
    }
  }


  // Initialize Ziplogs
  epoch_duration_ = config->epoch_duration();

  uint64_t clock = std::chrono::system_clock::now().time_since_epoch().count();

  log_wait_time_ = clock;
  auto network_delays = std::map<slog::RegionId, std::array<double, MAX_NUM_REGIONS>>();

  switch (config->log_type()) {
    case slog::internal::DETERMINISTIC_MERGE:
      LOG(INFO) << "Created DETERMINISTIC MERGE log";
      log_.reset(new DeterministicMergeLog(ziplog_manager_debug_, local_region, config->epoch_duration()));
      break;
    case slog::internal::FURTHEST_REGION:
      if (config->has_simulated_delays()){
        for (RegionId reg = 0; reg < config->num_regions(); reg++){
          network_delays[reg] = config->network_delays(reg);
        }
      }
      log_.reset(new PessimisticDMLog(ziplog_manager_debug_, local_region, config->epoch_duration(), config, network_delays));
      LOG(INFO) << "Created FURTHEST REGION log";

      break;
    case slog::internal::MULTI_SLOT:
      if (config->has_simulated_delays()){
        for (RegionId reg = 0; reg < config->num_regions(); reg++){
          network_delays[reg] = config->network_delays(reg);
        }
      }

      log_.reset(new MultiSlotLog(ziplog_manager_debug_, local_region, config->epoch_duration(), config, network_delays));
      LOG(INFO) << "Created MULTI_SLOT log";
      break;
    default:
      LOG(ERROR) << "A log type should be returned";
  }

}

void GLOG::OnInternalRequestReceived(EnvelopePtr&& env) {
  auto request = env->mutable_request();
  switch (request->type_case()) {
    case Request::kOrderEpoch:
      ProcessZiplogBatchOrder(std::move(env));
      break;
    case Request::kZipForwardBatch:
      ProcessZiplogBatchData(std::move(env));
      break;
    case Request::kZipFinishClient:
      RemoveClient(std::move(env));
      break;
    default:
      LOG(ERROR) << ziplog_manager_debug_ << "Unexpected request type received: \"" << CASE_NAME(request->type_case(), Request) << "\"";
  }

  AdvanceLog();
}

void GLOG::RemoveClient(EnvelopePtr&& env){
  auto remove_client_id = env->mutable_request()->mutable_zip_finish_client()->client_id();

  log_->RemoveClient(remove_client_id);
  log_->Reset();

  LOG(INFO) << ziplog_manager_debug_ << "Removed all slots from client " << CLIENT_ID_STR(remove_client_id);

  auto order_inform_env = NewEnvelope();


  auto remove_client_confirmation = order_inform_env->mutable_response()->mutable_remove_client_confirmation();
  remove_client_confirmation->set_client_id(remove_client_id);

  Send(std::move(order_inform_env), config()->order_machine_id(), kZipOrderChannel);
}


void GLOG::ProcessZiplogBatchOrder(EnvelopePtr&& env) {

  auto order_region_slots = env->mutable_request()->release_order_epoch();

  auto epoch_start = order_region_slots->epoch_start();

  // All regions get the same epoch
  log_->AddEpoch(order_region_slots->mutable_regions(0), epoch_start);

  delete order_region_slots;
}

void GLOG::ProcessZiplogBatchData(EnvelopePtr&& env) {
  auto local_region = config()->local_region();

  auto forward_batch_data = env->mutable_request()->mutable_zip_forward_batch();

  // Home of that batch
  auto batch_home = GET_REGION_ID(forward_batch_data->home());

  /*
  VLOG(2) << ziplog_manager_debug_ << "Batch " << forward_batch_data->generator_position() << "/"
          << CLIENT_ID_STR(forward_batch_data->generator()) << " arrived from home " << batch_home;
          */


  auto [from_region, from_replica, from_partition] = UnpackMachineId(env->from());
  bool first_time_region = from_region != local_region;

  if (first_time_region) {
    // If this batch comes from a different region, distribute it to other local replicas
    Send(*env, other_replicas_, kGLOGChannel);
    //VLOG(2) << ziplog_manager_debug_ << "Propagating Batch " << forward_batch_data->generator_position() << "/" << CLIENT_ID_STR(forward_batch_data->generator()) << " to other replicas";
  }

  auto log_flag = 2;

  // TODO -  We assume sequencers dont batch
  auto batch = forward_batch_data->mutable_batch_data(0);
  uint64_t clock = std::chrono::system_clock::now().time_since_epoch().count();
  batch->set_log_insertion(clock);
  batch->set_gsn_at_insertion(log_->current_slot_);
  for (auto txn_it = 0; txn_it < batch->transactions_size(); txn_it++){
    RECORD(batch->mutable_transactions(txn_it)->mutable_internal(), TransactionEvent::ENTER_GLOG);
  }

  if (batch->transactions_size() == 0 || batch->transactions(0).internal().noop()){
    // Noops and normal txns should be logged differently
    log_flag = 4;
  }

  VLOG(log_flag) << ziplog_manager_debug_ << "Received data for batch " << forward_batch_data->generator_position() << "/" <<
      CLIENT_ID_STR(forward_batch_data->generator()) << " (home = " << (int)batch_home << ") from "
          << MACHINE_ID_STR(env->from()) << ". Number of txns: " << batch->transactions_size()
          << ". First time region: " << first_time_region;

  // Record that txns have entered the log

  log_->AddBatch(forward_batch_data);
}

void GLOG::AdvanceLog() {
  // Advance local log

  while (log_->HasNextBatch()){
    auto [slot, next_batch, skip] = log_->NextBatch();

    if (next_batch != nullptr){
      if (per_thread_metrics_repo != nullptr){
        uint64_t clock = std::chrono::system_clock::now().time_since_epoch().count();

        if (config()->metric_options().logs()){

          if (next_batch->transactions_size() == 0){
            per_thread_metrics_repo->RecordLogProfiling(config()->local_machine_id(), next_batch->id(), -1, slot,
                                                        next_batch->log_insertion(), log_wait_time_, clock,
                                                        next_batch->gsn_at_insertion());
          } else {
            for (auto& txn : next_batch->transactions()) {
              const auto& internal = txn.internal();
              per_thread_metrics_repo->RecordLogProfiling(config()->local_machine_id(), next_batch->id(), internal.id(), slot,
                                                          next_batch->log_insertion(), log_wait_time_, clock,
                                                          next_batch->gsn_at_insertion());
            }
          }
        }

        log_wait_time_ = clock;
      }
    }

    // If the slot is not a skip or it requires client response, emit it
    if (!skip || next_batch != nullptr){
      VLOG(4) << ziplog_manager_debug_ << "Emmiting slot " << slot;
      EmitBatch(next_batch, slot);
    }
  }

}

void GLOG::SendBatches(){
  auto env = NewEnvelope();
  auto glog_batches = env->mutable_request()->mutable_glog_batch();
  for (auto & batch : batches_){
    for (int txn_index = batch->transactions_size() -1; txn_index >= 0; txn_index--){
      auto txn = batch->mutable_transactions(txn_index);
      auto& regions = txn->internal().involved_regions();
      bool is_local = false;
      for (int reg = 0; reg < regions.size(); reg++){
        if (regions[reg] == config()->local_region()){
          is_local = true;
        }
      }

      // If its not local, remove it
      if (!is_local){
        batch->mutable_transactions()->DeleteSubrange(txn_index, 1);
      } else {
          RECORD_WITH_TIME(txn->mutable_internal(), TransactionEvent::ENTER_GLOG_ORDER, batch->ordering_arrival());
          RECORD(txn->mutable_internal(), TransactionEvent::EXIT_GLOG);
        }
      }


    if (batch->transactions_size() > 0){

      auto glog_batch = glog_batches->add_batches();

      glog_batch->set_slot(unique_batch_id++);
      *glog_batch->mutable_txns() = *batch.get();

      /*
      // If region fault-tolerance is needed, replicate to other regions. For now, to everyone
      if (config()->replication_factor() > 1){
        Send(*env, other_regions_, kSequencerChannel);
      }

      // Only the glog on the replica's leader sends the actual txn batch
      if (config()->local_replica() == 0){
        env->mutable_request()->mutable_glog_batch()->set_allocated_txns(batch.get());
      }
*/
    }
  }
  Send(move(env), MakeMachineId(config()->local_region(), 0, config()->leader_partition_for_multi_home_ordering()), kSequencerChannel);
  batches_.clear();
}

void GLOG::EmitBatch(shared_ptr<Batch> batch, SlotId slot) {

  batches_.push_back(batch);

  if (batches_.size() == 1){
    NewTimedCallback(config()->mh_orderer_batch_duration(), [this]() {
      SendBatches();
    });
  }

}

}  // namespace slog