//
// Created by jrsoares on 29-03-2024.
//

#include "glog_stub.h"

#include "common/stub_utils.h"
#include "common/types.h"

namespace glog {

using slog::kZipTxnGenerator;

GlogStub::GlogStub(RegionId region, ReplicaId replica, PartitionId partition_id, ClientStubId id,
                   const std::shared_ptr<slog::Broker>& broker, const MetricsRepositoryManagerPtr& metrics_manager,
                   std::chrono::milliseconds poll_timeout, bool noop_debug)
    : NetworkedModule(broker->context(), broker->config(), broker->config()->stub_port(), slog::kGLOGStubChannel + id, metrics_manager, poll_timeout,
                      true /* is_long_sender */),
      id_(id),
      partition_id_(partition_id),
      region_(region),
      replica_(replica),
      stub_id_(MakeClientId(region_, replica_, partition_id, id)),
      epoch_duration_(config()->epoch_duration()),
      ema_({config()->smoothing(), config()->measurements(), config()->noops()}),
      noop_percentage_(config()->noops()),
      min_slots_(config()->min_slots()),
      noop_debug_(noop_debug),
      rounding_factor_(config()->rounding_factor()),
      num_regions_(config()->num_regions()){
  CHECK_GT(config()->min_slots(), 0) << "Minimum number of slots must be greater than 0";
  std::stringstream stub_id_debug;
  stub_id_debug << "[STUB_" << (int)region_ << "_" << (int)replica_ << "_" << partition_id_ << "_" << id_ << "]";
  stub_id_debug_ = stub_id_debug.str();
  batch_->set_home(config()->local_region());

  // Set template for noop txns
  noop_txn_.reset(new Transaction());
  noop_txn_->mutable_internal()->set_noop(true);

  // Initialize Batches


  LOG(INFO) << "Stub ID: " << CLIENT_ID_STR(MakeClientId(region_, replica_, partition_id_, id));

  auto local_region = config()->local_region();
  auto local_replica = config()->local_replica();
  auto local_partition = config()->local_partition();

  for (RegionId r = 0; r < num_regions_; r++) {

    send_batches_[r].reset(new Batch());

    send_batches_[r]->set_home(local_region);

    std::vector<MachineId> glog_replicas;


    if (static_cast<RegionId>(r) != local_region) {
      glog_replicas.push_back(MakeMachineId(r, local_replica, config()->leader_partition_for_multi_home_ordering()));
    } else {
      for (ReplicaId rep = 0; rep < config()->num_replicas(local_region); rep++) {
        glog_replicas.push_back(MakeMachineId(r, rep, config()->leader_partition_for_multi_home_ordering()));
      }
    }

    glog_replicas_[r] = glog_replicas;
  }

  usage_statistic_.start = std::chrono::high_resolution_clock::now().time_since_epoch().count();
  NewTimedCallback(microseconds(1000000),
                   [this] { RecordUsageStatistics(); });
}

void GlogStub::RecordUsageStatistics() {
  auto end = std::chrono::high_resolution_clock::now().time_since_epoch().count();
  usage_statistic_.end = end;

  if (per_thread_metrics_repo != nullptr) {
    per_thread_metrics_repo->RecordStubGoodputSize(stub_id_, usage_statistic_.start, usage_statistic_.end, usage_statistic_.goodput, usage_statistic_.badput,
                                                   usage_statistic_.region_slot_null, usage_statistic_.superposition_slot_null,
                                                   usage_statistic_.region_slot_used, usage_statistic_.superposition_slot_used);
  }

  usage_statistics_.push_back(UsageStatistics{usage_statistic_.start, usage_statistic_.end, usage_statistic_.goodput, usage_statistic_.badput,
  usage_statistic_.region_slot_null, usage_statistic_.superposition_slot_null, usage_statistic_.region_slot_used, usage_statistic_.superposition_slot_used});
  usage_statistic_.clear();
  usage_statistic_.start = end;


  NewTimedCallback(microseconds(1000000),
                   [this] { RecordUsageStatistics(); });
}
void GlogStub::Initialize() {
  LOG(INFO) << "Getting started!";


  // Kickstart system with defined slot number
  previous_ema_ = config()->start_slots();

  auto env = NewEnvelope();
  auto client_registration = env->mutable_request()->mutable_add_client();

  // Change with client id
  client_registration->set_client_id(stub_id_);
  client_registration->set_num_slots(std::max(config()->start_slots(), min_slots_));
  client_registration->set_region(region_);

  Send(std::move(env), config()->order_machine_id(), kZipOrderChannel);

  // Prepare request for updating rates
  update_rate_request_.set_num_slots(config()->start_slots());
  update_rate_request_.set_client_id(stub_id_);

}

void GlogStub::OnInternalRequestReceived(EnvelopePtr&& env) {
  auto request = env->mutable_request();
  // Now gets a message from the Forwarder
  switch (request->type_case()) {
    case slog::internal::Request::kForwardTxn:
      ProcessTxnForwardRequest(std::move(env));
      break;
    case slog::internal::Request::kZipFinishClient:
      // Deregister client from order service
      LOG(INFO) << stub_id_debug_ << " Begin closing the stub";

      FlagFinishStub();
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(request->type_case(), slog::internal::Request)
                 << "\"";
      break;
  }
}

void GlogStub::ProcessTxnForwardRequest(slog::EnvelopePtr&& env){
  statistics_.inserted_requests++;
  Transaction* txn = env->mutable_request()->mutable_forward_txn()->release_txn();
  RECORD(txn->mutable_internal(), TransactionEvent::ENTER_STUB);
  // Add new txn to batch;
  VLOG(1) << stub_id_debug_ << " Added transactions " << txn->internal().id() << "/"
          << CLIENT_ID_STR(txn->internal().client_id());

  queue_statistic_.AddTxn(txn->internal().id(), std::chrono::high_resolution_clock::now().time_since_epoch().count());

  // Add txn to relevant regions
  for (auto i = 0; i < txn->internal().involved_regions_size(); i++){
    if (i == txn->internal().involved_regions_size()-1){
      send_batches_[txn->internal().involved_regions(i)]->mutable_transactions()->AddAllocated(txn);
    } else {
      *send_batches_[txn->internal().involved_regions(i)]->mutable_transactions()->Add() = *txn;
    }
  }

}

void GlogStub::ProcessNewEpoch(slog::EnvelopePtr&& env) {
  auto new_epoch = env->mutable_response()->mutable_client_epoch();
  auto begin = std::chrono::high_resolution_clock::time_point(std::chrono::nanoseconds(new_epoch->begin()));

  auto next_send_timer =
      std::chrono::duration_cast<microseconds>(begin - std::chrono::high_resolution_clock::now());

  VLOG(2) << "[STUB] Got new epoch: Begin: " << begin.time_since_epoch().count()
          << " Slots assigned: " << new_epoch->num_slots();

  epoch_queues_.push({begin, assigned_slots_, assigned_slots_ += new_epoch->num_slots()});


  if (activate_log_process_) {
    activate_log_process_ = false;
    NewTimedCallback(microseconds(std::max(next_send_timer.count(), (long)0)),
                     [this] { ProcessEpoch(); });
  }

}

void GlogStub::OnInternalResponseReceived(EnvelopePtr&& env) {
  auto response = env->mutable_response();

  switch (response->type_case()) {
    case slog::internal::Response::kRemoveClientConfirmation: {
      LOG(INFO) << stub_id_debug_ << " Successfully removed client.";
      break;
    }
    case slog::internal::Response::kClientEpoch: {
      ProcessNewEpoch(std::move(env));
      break;
    }
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(response->type_case(), slog::internal::Response)
                 << "\"";
      break;
  }

}

void GlogStub::FinishStub() {
  // If we never registered, we can just terminate right away
  LOG(INFO) << stub_id_debug_ << " Informing Order to remove client from future epochs";
  stop_ = true;
  sleep(10);

  auto env = NewEnvelope();

  auto remove_client = env->mutable_request()->mutable_remove_client();

  remove_client->set_client_id(stub_id_);
  Send(std::move(env), config()->order_machine_id(), kZipOrderChannel);
}

void GlogStub::FlagFinishStub() {
  RecordStatistics();

  // If we are not waiting for any responses, we can exit
  if (pending_txns_ == 0) {
    // Wait 5 seconds for everything to stabilize
    LOG(INFO) << stub_id_debug_ << " is done";

    NewTimedCallback(microseconds(5000000), [this] { FinishStub(); });
  } else {
    LOG(INFO) << stub_id_debug_ << " Waiting for " << pending_txns_ << " pending txns";
    cooldown_ = true;
  }
}

void GlogStub::RecordStatistics() {
  uint64_t begin = current_epoch_.begin.time_since_epoch().count();

  if (per_thread_metrics_repo != nullptr) {
    per_thread_metrics_repo->RecordClientEpoch(stub_id_, begin, statistics_.inserted_requests, statistics_.sent_batches,
                                               statistics_.sent_noops, statistics_.sent_txns,
                                               update_rate_request_.num_slots(),
                                               current_epoch_.last - current_epoch_.first, false);
  }

  epoch_statistics_.push_back(EpochStatistics{stub_id_, begin, statistics_.inserted_requests, statistics_.sent_batches,
                                              statistics_.sent_noops, statistics_.sent_txns,
                                              update_rate_request_.num_slots(),
                                              current_epoch_.last - current_epoch_.first, false});
  statistics_.clear();
}
void GlogStub::ProcessEpoch() {
  if (!epoch_queues_.empty()) {
    if (epoch_queues_.front().begin < std::chrono::high_resolution_clock::now()) {
      LOG(INFO) << "PROCESSING EPOCH";
      // Update statistics
      if (!first_epoch_) {

        // TODO - Insert flag to allow for activation of slot measurement

        // Update number of slots
        double weight = (ema_.smoothing / (1 + ema_.measurements));
        double ema = ((double)statistics_.inserted_requests * weight) + previous_ema_ * (1 - weight);

        previous_ema_ = ema;

        unsigned long quantized =
            align(std::max(1UL, (unsigned long)(ema / (1.0 - noop_percentage_))), rounding_factor_);
        quantized = std::max(min_slots_, quantized);


        if (quantized >= min_slots_ && quantized != update_rate_request_.num_slots()) {
          VLOG(2) << stub_id_debug_ << " Requesting " << std::round<unsigned long>(quantized)
                  << " slots from the ordering service, ema: " << ema << " epoch_requests: " << epoch_requests_;
          update_rate_request_.set_num_slots((unsigned long)quantized);

          auto env = NewEnvelope();
          env->mutable_request()->mutable_update_rate()->CopyFrom(update_rate_request_);
          Send(std::move(env), config()->order_machine_id(), kZipOrderChannel);
        }

        RecordStatistics();

      } else {
        first_epoch_ = false;
      }

      // Move to next epoch
      current_epoch_ = epoch_queues_.front();

      epoch_queues_.pop();

      // Reactivate the send batch loop
      if (activate_send_batch_) {
        activate_send_batch_ = false;
        LOG(INFO) << stub_id_debug_ << "Activating send batch. Current epoch first: " << current_epoch_.first
                  << " Current Epoch last: " << current_epoch_.last;
        // We use a timed callback with time 0 to not stop process execution

        NewTimedCallback(microseconds(0), [this] { SendBatch(); });
      }

      if (!epoch_queues_.empty()) {
        auto next_epoch_timer = std::chrono::duration_cast<microseconds>(epoch_queues_.front().begin -
                                                                         std::chrono::high_resolution_clock::now());

        NewTimedCallback(microseconds((next_epoch_timer.count() > 0 ? next_epoch_timer.count() : 0)),
                         [this] { ProcessEpoch(); });
      } else {
        activate_log_process_ = true;
      }

    } else {
      auto next_epoch_timer = std::chrono::duration_cast<microseconds>(epoch_queues_.front().begin -
                                                                       std::chrono::high_resolution_clock::now());

      NewTimedCallback(microseconds((next_epoch_timer.count() > 0 ? next_epoch_timer.count() : 0)),
                       [this] { ProcessEpoch(); });
    }
  } else {
    activate_log_process_ = true;
  }
}

void GlogStub::SendBatch(bool check_noop) {
  // Stop sending batches if its time to shut down
  if (!stop_ && consumed_slots_ < current_epoch_.last) {
    auto now = std::chrono::high_resolution_clock::now();

    // auto ideal = current_epoch_.first + (current_epoch_.last - current_epoch_.first) *
    // std::chrono::duration_cast<microseconds>(now - current_epoch_.begin).count() / epoch_duration_.count();

    uint64_t ideal = static_cast<uint64_t>(std::floor(
        current_epoch_.first +
        (current_epoch_.last - current_epoch_.first) *
            (static_cast<double>(std::chrono::duration_cast<microseconds>(now - current_epoch_.begin).count()) /
             epoch_duration_.count())));

    // Send a message if it's time

    if (consumed_slots_ < ideal) {
      // If we were late on sending some requests, advance a bunch of slots
      auto advance_slots = std::min(ideal, current_epoch_.last) - consumed_slots_;

      auto env = NewEnvelope();
      auto batch_request = env->mutable_request()->mutable_zip_forward_batch();
      batch_request->set_slots(advance_slots);
      batch_request->set_generator(stub_id_);
      batch_request->set_home(config()->local_region());
      batch_request->set_generator_position(sent_batches_++);

      int null_slots = 0;
      for (int r = 0; r < num_regions_; r++){
        bool is_goodput = true;

        if (send_batches_[r]->transactions_size() == 0){
          //*send_batches_[r]->mutable_transactions()->Add() = *noop_txn_;
          is_goodput = false;
          null_slots++;
        } else {

          for (int txn_it = 0; txn_it < send_batches_[r]->transactions_size(); txn_it++) {
            auto txn = send_batches_[r]->mutable_transactions(txn_it);
            RECORD(txn->mutable_internal(), TransactionEvent::EXIT_STUB);
          }

          pending_txns_ += send_batches_[r]->transactions_size();
        }


        batch_request->mutable_batch_data()->AddAllocated(send_batches_[r].release());

        if (is_goodput){
          usage_statistic_.goodput += env->ByteSizeLong();
        } else {
          usage_statistic_.badput += env->ByteSizeLong();
        }

        if (r == num_regions_-1){
          Send(std::move(env), glog_replicas_[r], kGLOGChannel);
        } else {
          Send(*env, glog_replicas_[r], kGLOGChannel);
          batch_request->clear_batch_data();
        }

        send_batches_[r].reset(new Batch());
        send_batches_[r]->set_home(config()->local_region());
      }
      statistics_.sent_batches++;

      consumed_slots_ += advance_slots;

      usage_statistic_.region_slot_null += null_slots;
      usage_statistic_.region_slot_used += num_regions_ - null_slots;

      if (null_slots == num_regions_){
        usage_statistic_.superposition_slot_null += 1;
      } else {
        usage_statistic_.superposition_slot_used += 1;
      }

      if (advance_slots > 1){
        usage_statistic_.superposition_slot_null += (advance_slots-1);
        usage_statistic_.region_slot_null += (advance_slots-1)*num_regions_;
      }

      if (per_thread_metrics_repo != nullptr && config()->metric_options().stub_goodput_sample()) {
        auto now_ts = now.time_since_epoch().count();

        for (auto & p : queue_statistic_.txns){
          per_thread_metrics_repo->RecordStubQueue(p.first, p.second, now_ts);
        }

        queue_statistic_.clear();
      }

    }

    // setup next send
    if (consumed_slots_ < current_epoch_.last) {
      auto next_send = current_epoch_.begin +
                       microseconds(static_cast<uint64_t>(std::floor((consumed_slots_ + 1 - current_epoch_.first) *
                                                                     static_cast<double>(epoch_duration_.count()) /
                                                                     (current_epoch_.last - current_epoch_.first))));

      auto next_send_timer = std::chrono::duration_cast<microseconds>(next_send - now);

      NewTimedCallback(microseconds(next_send_timer.count() > 0 ? next_send_timer.count() : 0),
                       [this] { SendBatch(); });
    } else {
      // No more slots, restart on the next epoch
      // Since we already have these checks at the start of the send, just call send again
      NewTimedCallback(microseconds(0), [this] { SendBatch(); });
    }

  } else {
    // We ran out of slots, must wait for next epoch
    if (!epoch_queues_.empty()) {
      auto next_epoch_start = epoch_queues_.front().begin - std::chrono::high_resolution_clock::now();
      auto start_next_batch =
          next_epoch_start.count() > 0 ? std::chrono::duration_cast<microseconds>(next_epoch_start).count() : 0;

      LOG(INFO) << stub_id_debug_ << "Ran out of slots. Restarting batch loop in  " << start_next_batch
              << " microseconds";

      NewTimedCallback(microseconds(start_next_batch), [this] { SendBatch(true); });
    } else {
      LOG(INFO) << stub_id_debug_ << " Ran out of slots AND dont have next epoch. Waiting for next epoch to arrive";

      // Must activate the send batch loop when we get information regarding the next epoch
      activate_send_batch_ = true;
    }
  }
}

}  // namespace glog