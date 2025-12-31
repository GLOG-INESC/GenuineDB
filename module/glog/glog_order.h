//
// Created by jrsoares on 25-03-2024.
//

#ifndef GLOG_ORDER_H
#define GLOG_ORDER_H

#include "module/base/networked_module.h"
#include "common/types.h"
#include "common/configuration.h"
#include "common/proto_utils.h"
#include "proto/internal.pb.h"



namespace glog {

using slog::ConfigurationPtr;
using slog::EnvelopePtr;
using slog::MetricsRepositoryManagerPtr;
using namespace slog;

struct ClientAddress {
  MachineId client_machine_id;
  Channel stub_channel;
};

class GLOGOrder : public slog::NetworkedModule {
 public:
  GLOGOrder(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config, const MetricsRepositoryManagerPtr& metrics_manager,
        std::chrono::milliseconds poll_timeout_ms = slog::kModuleTimeout);

  std::string name() const override { return "GLOG_Order"; }

 protected:
  void Initialize() final;
  void OnInternalRequestReceived(EnvelopePtr&& env) final;
  void OnInternalResponseReceived(EnvelopePtr&& env) final;

 private:

  /** register new stub */
  void ProcessAddClientRequest(EnvelopePtr&& env);

  void ProcessUpdateRateRequest(EnvelopePtr&& env);

  void ProcessRemoveClientRequest(EnvelopePtr&& env);

  void ProcessRemoveClientResponse(EnvelopePtr&& env);

  /** sends the new epoch information to log managers */
  void AdvanceEpoch();

  /** Remove all information regarding a given client */
  void RemoveClient(ClientId client_id);

  /** create new message containing next epoch information*/
  slog::internal::ZiplogRegionSlots* NewEpochMessage(slog::internal::ZiplogNextEpochRequest* epoch, uint64_t num_clients, uint64_t gsn_base);

  void IssueEpoch();

  /** Number of slots already allocated */
  uint64_t gsn_base_ = 0;

  /** map from client IDs to their requested slots */
  std::unordered_map<ClientId, unsigned long> client_request_;

  /** map from client IDs to their addresses */
  std::unordered_map<ClientId, ClientAddress> client_addresses_;

  /** Current number of clients */
  uint64_t registered_clients_ = 0;

  /** map from client IDs to their shard */
  std::unordered_map<ClientId, std::pair<uint64_t, slog::MachineId>> client_shard_;

  /** All machine ids in the system */
  std::vector<MachineId> machine_ids_;

  /** Pending removals of clients */
  std::unordered_map<ClientId, unsigned long> client_removals_;

  bool bootstrap_ready_ = false;

  bool one_shot_experiment_ = true;

  int required_clients_ = 0;
};
}
#endif  // GLOG_ORDER_H
