#pragma once

#include <list>
#include <queue>
#include <random>

#include "common/configuration.h"
#include "common/metrics.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/base/networked_module.h"
#include "module/sequencer_components/batcher.h"

namespace slog {

class BatchReplicationHandler {

 public:
  BatchReplicationHandler(Batch* batch, int local_rep_factor, int global_rep_factor);
  BatchReplicationHandler(int local_rep_factor, int global_rep_factor);

  void AddAck(RegionId reg);
  void AddAck(Batch* batch, RegionId reg);
  bool ReplicationComplete();
  Batch* GetBatch();
  bool IsInitialized();
  void Initialize(int local_rep_factor, int global_rep_factor);

 private:
  Batch* batch_;
  int local_rep_factor_;
  int global_rep_factor_;
  bool initialized_ = false;

  // Tracker of how many regions confirmed to have globally replicated this batch
  int global_rep_ = 0;
  std::array<int, MAX_NUM_REGIONS> region_rep_factor = {};
};

/**
 * A Sequencer batches transactions before sending to the Log Manager.
 *
 * INPUT:  ForwardTxn
 *
 * OUTPUT: For a single-home txn, it is put into a batch. The ID of this batch is
 *         sent to the local paxos process for ordering. Simultaneously, this batch
 *         is sent to the Log Manager of all machines across all regions.
 *
 *         For a multi-home txn, a corresponding lock-only txn is created and then goes
 *         through the same process as a single-home txn above.
 */
using BatchBlock = std::array<BatchReplicationHandler, slog::NUM_ENTRIES_PER_BLOCK>;
static_assert(sizeof(BatchBlock) % slog::HUGE_PAGE_SIZE == 0);
using BatchBlocks = std::list<BatchBlock*>;

class Sequencer : public NetworkedModule {
 public:
  Sequencer(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
            const MetricsRepositoryManagerPtr& metrics_manager,
            std::chrono::milliseconds poll_timeout = kModuleTimeout);

  std::string name() const override { return "Sequencer"; }

 protected:
  void Initialize() final;
  void OnInternalRequestReceived(EnvelopePtr&& env) final;

 private:
  void ProcessForwardRequest(EnvelopePtr&& env);
  void ProcessForwardBatchRequest(EnvelopePtr&& env);
  void ProcessPingRequest(EnvelopePtr&& env);

  std::shared_ptr<Batcher> batcher_;
  ModuleRunner batcher_runner_;



  BatchBlocks batch_handler_;
  BatchBlocks::iterator batch_block_;
  BatchReplicationHandler* current_batch_ = nullptr;
  // Keeps track of the number of batches sent by GLOG
  uint64_t current_slot_ = 0;

  uint64_t batch_block_it_ = 0;
  uint64_t allocated_blocks_ = 1;

  uint64_t processed_blocks_ = 0;

  std::string sequencer_debug_;

};

}  // namespace slog