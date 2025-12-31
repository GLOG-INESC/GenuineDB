#pragma once

#include "common/configuration.h"
#include "common/metrics.h"
#include "common/sharder.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/base/networked_module.h"
#include "storage/metadata_initializer.h"

namespace tiga {

using slog::ConfigurationPtr;
using slog::EnvelopePtr;
using slog::MachineId;
using slog::MetricsRepositoryManagerPtr;
using slog::TxnId;
using slog::PartitionId;

class Coordinator : public slog::NetworkedModule {
 public:
  Coordinator(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
              const std::shared_ptr<slog::MetadataInitializer>& metadata_initializer,
              const MetricsRepositoryManagerPtr& metrics_manager,
              std::chrono::milliseconds poll_timeout_ms = slog::kModuleTimeout,
              bool debug_mode = false);

  std::string name() const override { return "Coordinator"; }

 protected:
  void OnInternalRequestReceived(EnvelopePtr&& env) final;
  void OnInternalResponseReceived(EnvelopePtr&& env) final;

 private:
  void StartNewTxn(EnvelopePtr&& env);
  void PrintStats();

  std::chrono::time_point<std::chrono::system_clock> GetTimestamp();

  const slog::SharderPtr sharder_;
  std::shared_ptr<slog::MetadataInitializer> metadata_initializer_;
  std::map<slog::RegionId, std::array<double, slog::MAX_NUM_REGIONS>> network_delays_;

  // Debug flag for testing
  bool debug_mode_;

  std::string coordinator_id_debug_;

};

}  // namespace janus