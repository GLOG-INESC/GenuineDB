//
// Created by jrsoares on 25-03-2024.
//
#include "module/glog/glog_order.h"
#include "common/constants.h"
#include "module/log_manager.h"
namespace glog {

using slog::kZipOrderChannel;
using std::move;

GLOGOrder::GLOGOrder(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
             const MetricsRepositoryManagerPtr& metrics_manager, std::chrono::milliseconds poll_timeout_ms)
   : NetworkedModule(context, config, config->order_port(), kZipOrderChannel, metrics_manager, poll_timeout_ms){}

/***********************************************
                Initialization
***********************************************/

void GLOGOrder::Initialize() {

  required_clients_ = config()->num_regions()*config()->num_replicas(0)*config()->num_stubs();

  if (!config()->shrink_stubs()){
    required_clients_ *= config()->num_partitions();
  }
  // Get all storage servers from config information
  for (int reg = 0; reg < config()->num_regions(); reg++){
    std::vector<MachineId> region_machines;
    for (int rep = 0; rep < config()->num_replicas(reg); rep ++){
        machine_ids_.push_back(MakeMachineId(reg, rep, config()->leader_partition_for_multi_home_ordering()));
    }
  }

  //Add epoch size to config
  if (config()->epoch_duration() > std::chrono::milliseconds(0)) {
    AdvanceEpoch();
  }
}

void GLOGOrder::ProcessAddClientRequest(slog::EnvelopePtr&& env) {
  auto client_info = env->mutable_request()->mutable_add_client();
  CHECK(client_request_.count(client_info->client_id()) == 0) << "Error: Client " << CLIENT_ID_STR(client_info->client_id()) << " already registered";

  LOG(INFO) << "Registered Client " << CLIENT_ID_STR(client_info->client_id()) << " from region " << client_info->region()
            << " connected to server with Machine id " << client_info->machine_id();

  client_request_[client_info->client_id()] = client_info->num_slots();

  client_addresses_[client_info->client_id()] = {env->from(), kGLOGStubChannel+GET_CLIENT_ID(client_info->client_id())};
  client_shard_[client_info->client_id()] = std::pair<RegionId, slog::MachineId>(client_info->region(), client_info->machine_id());

  registered_clients_++;
}

void GLOGOrder::ProcessUpdateRateRequest(slog::EnvelopePtr&& env) {

  auto client_info = env->mutable_request()->mutable_update_rate();

  CHECK(client_request_.count(client_info->client_id()) != 0) << " Error: Updating rate of unregistered Client " << CLIENT_ID_STR(client_info->client_id());

  VLOG(3) << "Updating rate of Client " << CLIENT_ID_STR(client_info->client_id()) << " to " << client_info->num_slots() << "slots";
  client_request_[client_info->client_id()] = client_info->num_slots();
}

void GLOGOrder::ProcessRemoveClientRequest(slog::EnvelopePtr&& env) {
  auto remove_client_info = env->mutable_request()->mutable_remove_client();
  auto remove_client_id = remove_client_info->client_id();

  CHECK(client_request_.count(remove_client_id) != 0) << " Error: Attempted to remove unregistered Client " << CLIENT_ID_STR(remove_client_id);

  LOG(INFO) << "[ORDER] Request to remove client " << CLIENT_ID_STR(remove_client_id);

  registered_clients_--;

  client_request_.erase(remove_client_id);
  client_shard_.erase(remove_client_id);

  /* Inform servers of client removal */
  auto client_removal_env = NewEnvelope();
  client_removal_env->mutable_request()->mutable_zip_finish_client()->set_client_id(remove_client_id);
  Send(std::move(client_removal_env), machine_ids_, kGLOGChannel);

  unsigned long pending_machines = machine_ids_.size();

  // Should never happen, but if there are no machines to send just answer the client
  if (pending_machines == 0){
    LOG(ERROR) << "No pending machines to send";
  } else {
    client_removals_[remove_client_id] = pending_machines;
  }
}

void GLOGOrder::ProcessRemoveClientResponse(EnvelopePtr&& env){
  auto removed_client_info = env->mutable_response()->mutable_remove_client_confirmation();
  auto removed_client_id = removed_client_info->client_id();

  client_removals_[removed_client_id]--;

  if (client_removals_[removed_client_id] == 0){
    RemoveClient(removed_client_id);
    client_removals_.erase(removed_client_id);
  }

  // After removing all clients, reset Order
  if (client_request_.empty() && client_removals_.empty()){
    client_addresses_.clear();
    client_shard_.clear();
    gsn_base_ = 0;
    bootstrap_ready_ = false;
    AdvanceEpoch();
  }
}
/***********************************************
              Internal Requests
***********************************************/

void GLOGOrder::OnInternalRequestReceived(EnvelopePtr&& env) {
  auto request = env->mutable_request();
  switch (request->type_case()) {
    case internal::Request::kAddClient: {
      ProcessAddClientRequest(std::move(env));
      break;
    }
    case internal::Request::kUpdateRate: {
      ProcessUpdateRateRequest(std::move(env));
      break;
    }
    case internal::Request::kRemoveClient:{
      ProcessRemoveClientRequest(std::move(env));
      break;
    }

    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(request->type_case(), internal::Request) << "\"";
      break;
  }

}

void GLOGOrder::OnInternalResponseReceived(EnvelopePtr&& env) {
  auto response = env->mutable_response();
  switch (response->type_case()) {
    case internal::Response::kRemoveClientConfirmation: {
      ProcessRemoveClientResponse(std::move(env));
      break;
    }
    default:
      LOG(ERROR) << "Unexpected response type received: \"" << CASE_NAME(response->type_case(), internal::Response) << "\"";
      break;
  }
  LOG(ERROR) << "Unexpected response type received: \"" << CASE_NAME(env->response().type_case(), slog::internal::Response)<< "\"";

}


void GLOGOrder::IssueEpoch() {
  if(!client_request_.empty()){
    auto next_epoch_begin = std::chrono::high_resolution_clock::now() ;
    //next_epoch_begin += config()->epoch_duration();

    if (one_shot_experiment_){
      next_epoch_begin += std::chrono::seconds(5);
    } else {
      next_epoch_begin += config()->epoch_duration();
    }

    auto env = NewEnvelope();
    auto epoch = env->mutable_request()->mutable_order_epoch();
    epoch->set_epoch_start(next_epoch_begin.time_since_epoch().count());

    auto slots = NewEpochMessage(epoch, registered_clients_, gsn_base_);

    // go over each client state and calculate that client's slots
    for (auto& [cid, num_slots]: client_request_) {
      // Inform client of its slots for the next epoch
      auto client_region = client_shard_[cid].first;

      auto client_env = NewEnvelope();

      auto next_epoch = client_env->mutable_response()->mutable_client_epoch();
      next_epoch->set_begin(next_epoch_begin.time_since_epoch().count());
      next_epoch->set_num_slots(num_slots);

      Send(std::move(client_env), client_addresses_[cid].client_machine_id, client_addresses_[cid].stub_channel);

      // Add this client slots to the epochs for each region

      slog::internal::ZiplogClientSlot* client_slots = slots->mutable_slots()->Add();
      client_slots->set_client_id(cid);
      client_slots->set_num_slots(num_slots);
      client_slots->set_client_region(client_region);

      // Clients in glog are always global
      client_slots->set_client_type(MULTI_HOME_OR_LOCK_ONLY);

      gsn_base_ += num_slots;

    }

    Send(std::move(env), machine_ids_, kGLOGChannel);

  }
}
void GLOGOrder::AdvanceEpoch(){

  if (client_request_.size() < required_clients_){
    // Poll every 500ms until we reach all slots

    NewTimedCallback(std::chrono::microseconds(500000), [this] {
      GLOGOrder::AdvanceEpoch();
    });
  } else {
    if (one_shot_experiment_){

      if (!bootstrap_ready_){
        bootstrap_ready_ = true;
        LOG(INFO) << "[ORDER] Sending the Epoch";
        IssueEpoch();
      }

    } else {
      NewTimedCallback(std::chrono::duration_cast<std::chrono::microseconds>(config()->epoch_duration()), [this] {
        IssueEpoch();
        AdvanceEpoch();
      });
    }
  }


}

slog::internal::ZiplogRegionSlots* GLOGOrder::NewEpochMessage(slog::internal::ZiplogNextEpochRequest* epoch, uint64_t num_clients, uint64_t gsn_base) {
  auto regionSlots = epoch->add_regions();
  regionSlots->set_gsn_base(gsn_base);
  regionSlots->set_num_clients(num_clients);
  return regionSlots;
}

void GLOGOrder::RemoveClient(slog::ClientId client_id) {

  auto env = NewEnvelope();

  auto client_remove_confirmation = env->mutable_response()->mutable_remove_client_confirmation();
  client_remove_confirmation->set_client_id(client_id);
  LOG(INFO) << "[ORDER] All servers removed client " << client_id;

  Send(std::move(env), client_addresses_[client_id].client_machine_id, client_addresses_[client_id].stub_channel);

  // Remove client information from all data structures
  client_addresses_.erase(client_id);
}
//

}