#pragma once


#include "common/configuration.h"
#include "common/metrics.h"
#include "common/types.h"
#include "module/base/networked_module.h"

#include "proto/transaction.pb.h"
#include "common/log/log.h"

namespace glog {

using namespace slog;

class GLOG : public NetworkedModule {
 public:
  static uint64_t MakeLogChannel(RegionId region) { return kMaxChannel + region; }

  GLOG(int id, const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
             const MetricsRepositoryManagerPtr& metrics_manager,
             std::chrono::milliseconds poll_timeout = kModuleTimeout);

  std::string name() const override { return "GLOG"; }

 protected:
  void OnInternalRequestReceived(EnvelopePtr&& env) final;

 private:
  //void ProcessBatchReplicationAck(EnvelopePtr&& env);
  void ProcessZiplogBatchOrder(EnvelopePtr&& env);
  void ProcessZiplogBatchData(EnvelopePtr&& env);
  void RemoveClient(EnvelopePtr&& env);
  void AdvanceLog();
  void EmitBatch(std::shared_ptr<Batch> batch, SlotId slot);
  void SendBatches();
  int id_;
  std::string ziplog_manager_debug_;
  std::unique_ptr<Log> log_;

  // Statistics information regarding blocking time for a given entry
  uint64_t log_wait_time_;

  //std::unordered_map<RegionId, std::unique_ptr<Log>> logs_;
  std::vector<MachineId> other_replicas_;
  std::vector<MachineId> other_regions_;

  std::vector<bool> need_ack_from_region_;
  std::vector<RegionId> regions_;
  uint64_t unique_batch_id = 0;
  // Ziplog info
  std::chrono::milliseconds epoch_duration_;

  std::map<ClientId, uint32_t> client_stuff_;

  std::vector<shared_ptr<Batch>> batches_;
};

}  // namespace slog

//AddSlot(start_gsn++, slots[j].second, current_time)