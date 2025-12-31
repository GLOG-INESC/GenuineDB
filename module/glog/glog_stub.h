//
// Created by jrsoares on 29-03-2024.
//

#ifndef GLOG_STUB_H
#define GLOG_STUB_H

#include <queue>

#include "common/proto_utils.h"
#include "module/base/networked_module.h"
#include "proto/transaction.pb.h"

namespace glog {

using namespace slog;

using std::chrono::microseconds;
using std::chrono::milliseconds;

struct epoch {
  /** timestamp of the beginning of the epoch */
  std::chrono::high_resolution_clock::time_point begin;

  /** index at the start of the epoch */
  unsigned long first = 0;

  /** index of the end of the epoch */
  unsigned long last = 0;

  /** A simple equality operator. */
  bool operator==(const epoch& other) { return begin == other.begin && first == other.first && last == other.last; }
};

/**
 * Contains the constants used for Exponential Moving Average (EMA) rate estimation.
 */
struct ema {
  double smoothing;

  /** Number of past measurements to be considering */
  uint64_t measurements;

  /** How much should the number of slots change */
  double increment;
};

struct EpochStatistics {
  uint64_t stub_id;
  uint64_t time;
  int inserted_requests = 0;
  int sent_batches = 0;
  int sent_noops = 0;
  int sent_txns = 0;
  uint64_t slots_requested;
  uint64_t current_slots;
  bool sh_stub;

  void clear() {
    inserted_requests = 0;
    sent_batches = 0;
    sent_noops = 0;
    sent_txns = 0;
  }
};

struct UsageStatistics {
  int64_t start = 0;
  int64_t end = 0;
  uint64_t goodput = 0;
  uint64_t badput = 0;

  uint64_t region_slot_null = 0;
  uint64_t superposition_slot_null = 0;

  uint64_t region_slot_used = 0;
  uint64_t superposition_slot_used = 0;

  void clear() {
    end = 0;
    goodput = 0;
    badput = 0;

    region_slot_null = 0;
    superposition_slot_null = 0;
    region_slot_used = 0;
    superposition_slot_used = 0;
  }
};


struct QueueStatistics {

  std::vector<std::pair<TxnId, int64_t >> txns;
  int64_t end;

  void AddTxn(TxnId txn_id, int64_t start){
    txns.emplace_back(txn_id, start);
  }

  void clear() {
    txns.clear();
  }
};

class GlogStub : public slog::NetworkedModule {
 public:
  GlogStub(RegionId region, ReplicaId replica, PartitionId generator_id, ClientStubId id,
           const std::shared_ptr<slog::Broker>& broker, const MetricsRepositoryManagerPtr& metrics_manager,
           std::chrono::milliseconds poll_timeout = slog::kModuleTimeout, bool noop_debug = false);

  std::string name() const override { return "GlogStub"; }
  std::vector<EpochStatistics> epoch_statistics() const { return epoch_statistics_; }

 protected:
  void Initialize() final;
  void OnInternalRequestReceived(EnvelopePtr&& env) final;
  void OnInternalResponseReceived(EnvelopePtr&& env) final;

 private:
  void ProcessNewEpoch(EnvelopePtr&& env);
  void ProcessTxnForwardRequest(EnvelopePtr&& env);
  void RecordUsageStatistics();
  void ProcessEpoch();
  void SendBatch(bool check_noop = false);
  void FinishStub();
  void FlagFinishStub();
  void RecordStatistics();


  std::array<std::unique_ptr<Batch>, MAX_NUM_REGIONS> send_batches_;

  std::unique_ptr<Transaction> noop_txn_;

  Batch* batch_ = new Batch();
  uint32_t sent_batches_ = 0;
  internal::UpdateRateRequest update_rate_request_;

  std::map<RegionId, std::vector<MachineId>> glog_replicas_;

  std::vector<slog::api::Request*> batches_;
  ClientStubId id_;
  PartitionId partition_id_;
  slog::RegionId region_;
  slog::ReplicaId replica_;
  ClientId stub_id_;
  // Used for identifying stub in debugging
  string stub_id_debug_;

  std::queue<epoch> epoch_queues_;
  microseconds epoch_duration_;

  epoch current_epoch_;
  uint64_t assigned_slots_ = 0;
  uint64_t consumed_slots_ = 0;

  /** Flag to activate/deactivate Noops */
  bool activate_send_batch_ = true;
  bool activate_log_process_ = true;

  /** parameters for EMA rate estimation */
  uint64_t epoch_requests_ = 0;
  ema ema_;
  double previous_ema_ = 0;
  double noop_percentage_;
  uint64_t min_slots_ = 1;

  bool first_epoch_ = true;
  bool first_request_ = true;
  bool waiting_slots_ = false;

  uint64_t noops_ = 0;
  bool noop_debug_;

  bool stop_ = false;
  bool cooldown_ = false;

  int pending_txns_ = 0;
  // Metrics
  EpochStatistics statistics_;
  UsageStatistics usage_statistic_;
  QueueStatistics queue_statistic_;

  std::vector<EpochStatistics> epoch_statistics_;
  std::vector<UsageStatistics> usage_statistics_;

  int rounding_factor_;

  std::queue<Transaction*> await_txns_;

  int num_regions_;
};

}  // namespace glog
#endif  // GLOG_STUB_H
