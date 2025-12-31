#include "metrics.h"

#include <algorithm>

#include "common/csv_writer.h"
#include "common/proto_utils.h"
#include "common/string_utils.h"
#include "proto/internal.pb.h"
#include "version.h"

using std::list;
using std::pair;
using std::vector;
using std::chrono::system_clock;

namespace slog {

class Sampler {
  constexpr static uint32_t kSampleMaskSize = 1 << 8;
  using sample_mask_t = std::array<bool, kSampleMaskSize>;

  sample_mask_t sample_mask_;
  vector<uint8_t> sample_count_;

 public:
  Sampler(int sample_rate, size_t num_keys) : sample_count_(num_keys, 0) {
    sample_mask_.fill(false);
    for (uint32_t i = 0; i < sample_rate * kSampleMaskSize / 100; i++) {
      sample_mask_[i] = true;
    }
    auto rd = std::random_device{};
    auto rng = std::default_random_engine{rd()};
    std::shuffle(sample_mask_.begin(), sample_mask_.end(), rng);
  }

  bool IsChosen(size_t key) {
    DCHECK_LT(sample_count_[key], sample_mask_.size());
    return sample_mask_[sample_count_[key]++];
  }
};

class TransactionEventMetrics {
 public:
  TransactionEventMetrics(int sample_rate, uint32_t local_region, uint32_t local_partition)
      : sampler_(sample_rate, TransactionEvent_descriptor()->value_count()),
        local_region_(local_region),
        local_partition_(local_partition) {}

  system_clock::time_point Record(TxnId txn_id, TransactionEvent event) {
    auto now = system_clock::now();
    auto sample_index = static_cast<size_t>(event);
    if (sampler_.IsChosen(sample_index)) {
      txn_events_.push_back({.time = now.time_since_epoch().count(),
                             .region = local_region_,
                             .partition = local_partition_,
                             .txn_id = txn_id,
                             .event = event});
    }
    return now;
  }

  struct Data {
    int64_t time;  // nanosecond since epoch
    uint32_t region;
    uint32_t partition;
    TxnId txn_id;
    TransactionEvent event;
  };

  list<Data>& data() { return txn_events_; }

  static void WriteToDisk(const std::string& dir, const list<Data>& data) {
    CSVWriter txn_events_csv(dir + "/events.csv", {"txn_id", "event", "time", "partition", "region"});
    for (const auto& d : data) {
      txn_events_csv << d.txn_id << ENUM_NAME(d.event, TransactionEvent) << d.time << d.partition << d.region
                     << csvendl;
    }
  }

 private:
  Sampler sampler_;
  uint32_t local_region_;
  uint32_t local_partition_;
  list<Data> txn_events_;
};

class DeadlockResolverRunMetrics {
 public:
  DeadlockResolverRunMetrics(int sample_rate, uint32_t local_region, uint32_t local_partition)
      : sampler_(sample_rate, 1), local_region_(local_region), local_partition_(local_partition) {}

  void Record(int64_t runtime, size_t unstable_graph_sz, size_t stable_graph_sz, size_t deadlocks_resolved,
              int64_t graph_update_time) {
    if (sampler_.IsChosen(0)) {
      data_.push_back({.time = system_clock::now().time_since_epoch().count(),
                       .partition = local_partition_,
                       .region = local_region_,
                       .runtime = runtime,
                       .unstable_graph_sz = unstable_graph_sz,
                       .stable_graph_sz = stable_graph_sz,
                       .deadlocks_resolved = deadlocks_resolved,
                       .graph_update_time = graph_update_time});
    }
  }

  struct Data {
    int64_t time;  // nanosecond since epoch
    uint32_t partition;
    uint32_t region;
    int64_t runtime;  // nanosecond
    size_t unstable_graph_sz;
    size_t stable_graph_sz;
    size_t deadlocks_resolved;
    int64_t graph_update_time;
  };
  list<Data>& data() { return data_; }

  static void WriteToDisk(const std::string& dir, const list<Data>& data) {
    CSVWriter deadlock_resolver_csv(dir + "/deadlock_resolver.csv",
                                    {"time", "partition", "region", "runtime", "unstable_graph_sz", "stable_graph_sz",
                                     "deadlocks_resolved", "graph_update_time"});
    for (const auto& d : data) {
      deadlock_resolver_csv << d.time << d.partition << d.region << d.runtime << d.unstable_graph_sz
                            << d.stable_graph_sz << d.deadlocks_resolved << d.graph_update_time << csvendl;
    }
  }

 private:
  Sampler sampler_;
  uint32_t local_region_;
  uint32_t local_partition_;
  list<Data> data_;
};

class DeadlockResolverDeadlockMetrics {
  using edge_t = std::pair<uint64_t, uint64_t>;

 public:
  DeadlockResolverDeadlockMetrics(int sample_rate, bool with_details, uint32_t local_region, uint32_t local_partition)
      : sampler_(sample_rate, 1),
        with_details_(with_details),
        local_region_(local_region),
        local_partition_(local_partition) {}

  void Record(int num_vertices, const vector<edge_t>& edges_removed, const vector<edge_t>& edges_added) {
    if (sampler_.IsChosen(0)) {
      data_.push_back({.time = system_clock::now().time_since_epoch().count(),
                       .partition = local_partition_,
                       .region = local_region_,
                       .num_vertices = num_vertices,
                       .edges_removed = (with_details_ ? edges_removed : vector<edge_t>{}),
                       .edges_added = (with_details_ ? edges_added : vector<edge_t>{})});
    }
  }

  struct Data {
    int64_t time;  // nanosecond since epoch
    uint32_t partition;
    uint32_t region;
    int num_vertices;
    vector<edge_t> edges_removed;
    vector<edge_t> edges_added;
  };
  list<Data>& data() { return data_; }

  static void WriteToDisk(const std::string& dir, const list<Data>& data, bool with_details) {
    if (with_details) {
      CSVWriter deadlocks_csv(dir + "/deadlocks.csv", {"time", "partition", "region", "vertices", "removed", "added"});
      for (const auto& d : data) {
        deadlocks_csv << d.time << d.partition << d.region << d.num_vertices << Join(d.edges_removed)
                      << Join(d.edges_added) << csvendl;
      }
    } else {
      CSVWriter deadlocks_csv(dir + "/deadlocks.csv", {"time", "partition", "region", "vertices"});
      for (const auto& d : data) {
        deadlocks_csv << d.time << d.partition << d.region << d.num_vertices << csvendl;
      }
    }
  }

 private:
  Sampler sampler_;
  bool with_details_;
  uint32_t local_region_;
  uint32_t local_partition_;
  list<Data> data_;
};

class LogManagerLogs {
 public:
  void Record(uint32_t region, BatchId batch_id, TxnId txn_id, int64_t txn_timestamp,
              int64_t mh_depart_from_coordinator_time, int64_t mh_arrive_at_home_time,
              int64_t mh_enter_local_batch_time) {
    approx_global_log_.push_back({.region = region,
                                  .txn_id = txn_id,
                                  .batch_id = batch_id,
                                  .txn_timestamp = txn_timestamp,
                                  .mh_depart_from_coordinator_time = mh_depart_from_coordinator_time,
                                  .mh_arrive_at_home_time = mh_arrive_at_home_time,
                                  .mh_enter_local_batch_time = mh_enter_local_batch_time});
  }
  struct Data {
    uint32_t region;
    TxnId txn_id;
    BatchId batch_id;
    int64_t txn_timestamp;
    int64_t mh_depart_from_coordinator_time;
    int64_t mh_arrive_at_home_time;
    int64_t mh_enter_local_batch_time;
  };
  const vector<Data>& global_log() const { return approx_global_log_; }

  static void WriteToDisk(const std::string& dir, const vector<Data>& global_log) {
    CSVWriter approx_global_log_csv(dir + "/global_log.csv",
                                    {"region", "batch_id", "txn_id", "timestamp", "depart_from_coordinator",
                                     "arrive_at_home", "enter_local_batch"});
    for (const auto& e : global_log) {
      approx_global_log_csv << e.region << e.batch_id << e.txn_id << e.txn_timestamp
                            << e.mh_depart_from_coordinator_time << e.mh_arrive_at_home_time
                            << e.mh_enter_local_batch_time << csvendl;
    }
  }

 private:
  vector<Data> approx_global_log_;
};

class OrderingOverheadMetrics {
 public:
  OrderingOverheadMetrics(int sample_rate) : sampler_(sample_rate, 1) {}

  void Record(int64_t ordering_overhead) {
    if (sampler_.IsChosen(0)) {
      data_.push_back({ordering_overhead});
    }
  }
  struct Data {
    int64_t ordering_overhead;
  };

  static void WriteToDisk(const std::string& dir, const list<Data>& ordering_overheads) {
    CSVWriter approx_global_log_csv(dir + "/ordering_overheads.csv", {"latency"});
    for (const auto& e : ordering_overheads) {
      approx_global_log_csv << e.ordering_overhead << csvendl;
    }
  }

  list<Data>& data() { return data_; }

 private:
  Sampler sampler_;
  list<Data> data_;
};

class ForwSequLatencyMetrics {
 public:
  ForwSequLatencyMetrics(int sample_rate) : sampler_(sample_rate, 1) {}

  void Record(uint32_t dst, int64_t src_time, int64_t dst_time, int64_t src_recv_time, int64_t avg_time) {
    if (sampler_.IsChosen(0)) {
      data_.push_back({.dst = dst,
                       .src_time = src_time,
                       .dst_time = dst_time,
                       .src_recv_time = src_recv_time,
                       .avg_time = avg_time});
    }
  }

  struct Data {
    uint32_t dst;
    int64_t src_time;
    int64_t dst_time;
    int64_t src_recv_time;
    int64_t avg_time;
  };
  list<Data>& data() { return data_; }

  static void WriteToDisk(const std::string& dir, const list<Data>& data) {
    CSVWriter forw_sequ_latency_csv(dir + "/forw_sequ_latency.csv",
                                    {"dst", "src_time", "dst_time", "src_recv_time", "avg_time"});
    for (const auto& d : data) {
      forw_sequ_latency_csv << d.dst << d.src_time << d.dst_time << d.src_recv_time << d.avg_time << csvendl;
    }
  }

 private:
  Sampler sampler_;
  list<Data> data_;
};

class ClockSyncMetrics {
 public:
  ClockSyncMetrics(int sample_rate) : sampler_(sample_rate, 1) {}

  void Record(uint32_t dst, int64_t src_time, int64_t dst_time, int64_t src_recv_time, int64_t local_slog_time,
              int64_t avg_latency, int64_t new_offset) {
    if (sampler_.IsChosen(0)) {
      data_.push_back({.dst = dst,
                       .src_time = src_time,
                       .dst_time = dst_time,
                       .src_recv_time = src_recv_time,
                       .local_slog_time = local_slog_time,
                       .avg_latency = avg_latency,
                       .new_offset = new_offset});
    }
  }

  struct Data {
    uint32_t dst;
    int64_t src_time;
    int64_t dst_time;
    int64_t src_recv_time;
    int64_t local_slog_time;
    int64_t avg_latency;
    int64_t new_offset;
  };
  list<Data>& data() { return data_; }

  static void WriteToDisk(const std::string& dir, const list<Data>& data) {
    CSVWriter clock_sync_csv(dir + "/clock_sync.csv", {"dst", "src_time", "dst_time", "src_recv_time",
                                                       "local_slog_time", "avg_latency", "new_offset"});
    for (const auto& d : data) {
      clock_sync_csv << d.dst << d.src_time << d.dst_time << d.src_recv_time << d.local_slog_time << d.avg_latency
                     << d.new_offset << csvendl;
    }
  }

 private:
  Sampler sampler_;
  list<Data> data_;
};

class BatchMetrics {
 public:
  BatchMetrics(int sample_rate) : sampler_(sample_rate, 1) {}

  void Record(BatchId batch_id, size_t batch_size, int64_t batch_duration) {
    if (sampler_.IsChosen(0)) {
      data_.push_back({.batch_id = batch_id, .batch_size = batch_size, .batch_duration = batch_duration});
    }
  }

  struct Data {
    BatchId batch_id;
    size_t batch_size;
    int64_t batch_duration;
  };
  list<Data>& data() { return data_; }

  static void WriteToDisk(const std::string& file_path, const list<Data>& data) {
    CSVWriter batch_csv(file_path, {"batch_id", "batch_size", "batch_duration"});
    for (const auto& d : data) {
      batch_csv << d.batch_id << d.batch_size << d.batch_duration << csvendl;
    }
  }

 private:
  Sampler sampler_;
  list<Data> data_;
};

class TxnTimestampMetrics {
 public:
  TxnTimestampMetrics(int sample_rate) : sampler_(sample_rate, 1) {}

  void Record(TxnId txn_id, uint32_t from, int64_t txn_timestamp, int64_t server_time) {
    if (sampler_.IsChosen(0)) {
      data_.push_back({.txn_id = txn_id, .from = from, .txn_timestamp = txn_timestamp, .server_time = server_time});
    }
  }

  struct Data {
    TxnId txn_id;
    uint32_t from;
    int64_t txn_timestamp;
    int64_t server_time;
  };
  list<Data>& data() { return data_; }

  static void WriteToDisk(const std::string& dir, const list<Data>& data) {
    CSVWriter txn_timestamps_csv(dir + "/txn_timestamps.csv", {"txn_id", "from", "txn_timestamp", "server_time"});
    for (const auto& d : data) {
      txn_timestamps_csv << d.txn_id << d.from << d.txn_timestamp << d.server_time << csvendl;
    }
  }

 private:
  Sampler sampler_;
  list<Data> data_;
};

class GenericMetrics {
 public:
  GenericMetrics(int sample_rate, uint32_t local_region, uint32_t local_partition)
      : sampler_(sample_rate, 1), local_region_(local_region), local_partition_(local_partition) {}

  void Record(int type, int64_t time, int64_t data) {
    if (sampler_.IsChosen(0)) {
      data_.push_back(
          {.time = time, .data = data, .type = type, .region = local_region_, .partition = local_partition_});
    }
  }

  struct Data {
    int64_t time;  // nanosecond since epoch
    int64_t data;
    int type;
    uint32_t region;
    uint32_t partition;
  };

  list<Data>& data() { return data_; }

  static void WriteToDisk(const std::string& dir, const list<Data>& data) {
    CSVWriter generic_csv(dir + "/generic.csv", {"type", "time", "data", "partition", "region"});
    for (const auto& d : data) {
      generic_csv << d.type << d.time << d.data << d.partition << d.region << csvendl;
    }
  }

 private:
  Sampler sampler_;
  uint32_t local_region_;
  uint32_t local_partition_;
  list<Data> data_;
};

class ClientEpochMetrics {
 public:
  ClientEpochMetrics(int sample_rate) : sampler_(sample_rate, 1) {}

  void Record(uint64_t stub_id, uint64_t time, int inserted_requests, int sent_requests, int sent_noops, int sent_txns,
              uint64_t slots_requested, uint64_t current_slots, bool sh_stub) {
    if (sampler_.IsChosen(0)) {
      data_.push_back({.stub_id = stub_id,
                       .time = time,
                       .inserted_requests = inserted_requests,
                       .sent_requests = sent_requests,
                       .sent_noops = sent_noops,
                       .sent_txns = sent_txns,

                       .slots_requested = slots_requested,
                       .current_slots = current_slots,

                       .sh_stub = sh_stub});
    }
  }

  struct Data {
    uint64_t stub_id;
    uint64_t time;
    int inserted_requests;
    int sent_requests;
    int sent_noops;
    int sent_txns;
    uint64_t slots_requested;
    uint64_t current_slots;
    bool sh_stub;
  };
  list<Data>& data() { return data_; }

  static void WriteToDisk(const std::string& file_path, const list<Data>& data) {
    CSVWriter client_epoch_csv(file_path, {"stub_id", "time", "inserted_requests", "sent_requests", "sent_noops",
                                           "sent_txns", "slots_requested", "current_slots", "sh_stub"});
    for (const auto& d : data) {
      client_epoch_csv << d.stub_id << d.time << d.inserted_requests << d.sent_requests << d.sent_noops << d.sent_txns
                       << d.slots_requested << d.current_slots << d.sh_stub << csvendl;
    }
  }

 private:
  Sampler sampler_;
  list<Data> data_;
};

class TxnLockMetrics {
 public:
  TxnLockMetrics(int sample_rate) : sampler_(sample_rate, 1) {}

  void Record(TxnId txn_id, std::string holder_txns, std::string queue_txns) {
    if (sampler_.IsChosen(0)) {
      data_.push_back({.txn_id = txn_id, .holder_txns = holder_txns, .queue_txns = queue_txns});
    }
  }

  struct Data {
    TxnId txn_id;
    std::string holder_txns;
    std::string queue_txns;
  };

  list<Data>& data() { return data_; }

  static void WriteToDisk(const std::string& file_path, const list<Data>& data) {
    CSVWriter client_epoch_csv(file_path, {"txn_id", "holder_txns", "queue_txns"});
    for (const auto& d : data) {
      client_epoch_csv << d.txn_id << d.holder_txns << d.queue_txns << csvendl;
    }
  }

 private:
  Sampler sampler_;
  list<Data> data_;
};

class TxnLockArrivalMetrics {
 public:
  TxnLockArrivalMetrics(int sample_rate) : sampler_(sample_rate, 1) {}

  void Record(TxnId txn_id, uint64_t got_request, uint64_t unlocked_all) {
    if (sampler_.IsChosen(0)) {
      data_.push_back({.txn_id = txn_id, .got_request = got_request, .unlocked_all = unlocked_all});
    }
  }

  struct Data {
    TxnId txn_id;
    uint64_t got_request;
    uint64_t unlocked_all;
  };

  list<Data>& data() { return data_; }

  static void WriteToDisk(const std::string& file_path, const list<Data>& data) {
    CSVWriter client_epoch_csv(file_path, {"txn_id", "got_unlock", "unlocked_all"});
    for (const auto& d : data) {
      client_epoch_csv << d.txn_id << d.got_request << d.unlocked_all << csvendl;
    }
  }

 private:
  Sampler sampler_;
  list<Data> data_;
};

class ActiveRedirectsMetrics {
 public:
  ActiveRedirectsMetrics(int sample_rate) : sampler_(sample_rate, 1) {}
  void Record(uint64_t worker_id, uint64_t redirect_change_time, uint64_t active_redirects) {
    if (sampler_.IsChosen(0)) {
      data_.push_back(
          {.worker_id = worker_id, .time_updated = redirect_change_time, .active_redirects = active_redirects});
    }
  }

  struct Data {
    uint64_t worker_id;
    uint64_t time_updated;
    uint64_t active_redirects;
  };

  list<Data>& data() { return data_; }

  static void WriteToDisk(const std::string& file_path, const list<Data>& data) {
    CSVWriter active_redirects_csv(file_path, {"worker_id", "timestamp", "active_redirects"});
    for (const auto& d : data) {
      active_redirects_csv << d.worker_id << d.time_updated << d.active_redirects << csvendl;
    }
  }

 private:
  Sampler sampler_;
  list<Data> data_;
};

class StubGoodputMetrics {
 public:
  StubGoodputMetrics(int sample_rate) : sampler_(sample_rate, 1) {}
  void Record(uint64_t stub_id, int64_t start_timestamp, int64_t end_timestamp, uint64_t goodput, uint64_t badput,
              uint64_t region_slot_null, uint64_t superposition_slot_null, uint64_t region_slot_used,
              uint64_t superposition_slot_used) {
    if (sampler_.IsChosen(0)) {
      data_.push_back({.stub_id = stub_id,
                       .start_timestamp = start_timestamp,
                       .end_timestamp = end_timestamp,
                       .goodput = goodput,
                       .badput = badput,
                       .region_slot_null = region_slot_null,
                       .superposition_slot_null = superposition_slot_null,
                       .region_slot_used = region_slot_used,
                       .superposition_slot_used = superposition_slot_used});
    }
  }

  struct Data {
    uint64_t stub_id;
    int64_t start_timestamp;
    int64_t end_timestamp;
    uint64_t goodput;
    uint64_t badput;

    uint64_t region_slot_null;
    uint64_t superposition_slot_null;
    uint64_t region_slot_used;
    uint64_t superposition_slot_used;
  };

  list<Data>& data() { return data_; }

  static void WriteToDisk(const std::string& file_path, const list<Data>& data) {
    if (data.empty()) {
      return;
    }

    CSVWriter stub_goodput_csv(file_path, {"start_timestamp", "end_timestamp", "goodput", "badput", "region_slot_null",
                                           "superposition_slot_null", "region_slot_used", "superposition_slot_used"});
    for (const auto& d : data) {
      stub_goodput_csv << d.start_timestamp << d.end_timestamp << d.goodput << d.badput << d.region_slot_null
                       << d.superposition_slot_null << d.region_slot_used << d.superposition_slot_used << csvendl;
    }
  }

 private:
  Sampler sampler_;
  list<Data> data_;
};

class StubQueueMetrics {
 public:
  StubQueueMetrics(int sample_rate) : sampler_(sample_rate, 1) {}
  void Record(uint64_t txn_id, int64_t start_timestamp, int64_t end_timestamp) {
    if (sampler_.IsChosen(0)) {
      data_.push_back({.txn_id = txn_id,
                       .start_timestamp = start_timestamp,
                       .end_timestamp = end_timestamp
                       });
    }
  }

  struct Data {
    uint64_t txn_id;
    int64_t start_timestamp;
    int64_t end_timestamp;
  };

  list<Data>& data() { return data_; }

  static void WriteToDisk(const std::string& file_path, const list<Data>& data) {
    if (data.empty()) {
      return;
    }
    CSVWriter stub_queue_csv(file_path, {"txn_id", "start_timestamp", "end_timestamp"});
    for (const auto& d : data) {
      stub_queue_csv << d.txn_id << d.start_timestamp << d.end_timestamp << csvendl;
    }
  }

 private:
  Sampler sampler_;
  list<Data> data_;
};

class TigaSpeculationMetrics {
public:
  TigaSpeculationMetrics(int sample_rate) :sampler_(sample_rate, 1) {}
  void Record(uint64_t txn_id, bool successful_speculation) {
    if (sampler_.IsChosen(0)) {
      data_.push_back({.txn_id = txn_id,
                       .successful_speculation = successful_speculation
                       });
    }
  }

  struct Data {
    uint64_t txn_id;
    bool successful_speculation;
  };

  list<Data>& data() { return data_; }

  static void WriteToDisk(const std::string& file_path, const list<Data>& data) {
    if (data.empty()) {
      return;
    }
    CSVWriter stub_queue_csv(file_path, {"txn_id", "successful_speculation"});
    for (const auto& d : data) {
      stub_queue_csv << d.txn_id << d.successful_speculation << csvendl;
    }
  }

private:
  Sampler sampler_;
  list<Data> data_;
};

/** Metrics regarding how the log is being filled
 * and how much time it takes to be processed */
class LogProfilingMetrics {
 public:
  LogProfilingMetrics(int sample_rate) : sampler_(sample_rate, 1) {}

  void Record(MachineId machine_id, BatchId batch_id, TxnId txn_id, uint64_t gsn, uint64_t insertion_timestamp,
              uint64_t waiting_time, uint64_t processing_time, uint64_t gsn_at_insertion) {
    if (sampler_.IsChosen(0)) {
      data_.push_back({.machine_id = machine_id,
                       .batch_id = batch_id,
                       .txn_id = txn_id,
                       .gsn = gsn,
                       .insertion = insertion_timestamp,
                       .wait = waiting_time,
                       .processing = processing_time,
                       .gsn_at_insertion = gsn_at_insertion});
    }
  }

  struct Data {
    MachineId machine_id;
    BatchId batch_id;
    TxnId txn_id;
    uint64_t gsn;
    uint64_t insertion;
    uint64_t wait;
    uint64_t processing;
    uint64_t gsn_at_insertion;
  };

  list<Data>& data() { return data_; }

  static void WriteToDisk(const std::string& file_path, const list<Data>& data) {
    CSVWriter log_profiling_csv(
        file_path, {"machine_id", "batch_id", "txn_id", "gsn", "gsn_at_insertion", "insertion", "wait", "processing"});
    for (const auto& d : data) {
      log_profiling_csv << d.machine_id << d.batch_id << d.txn_id << d.gsn << d.gsn_at_insertion << d.insertion
                        << d.wait << d.processing << csvendl;
    }
  }

 private:
  Sampler sampler_;
  list<Data> data_;
};

struct AllMetrics {
  TransactionEventMetrics txn_event_metrics;
  DeadlockResolverRunMetrics deadlock_resolver_run_metrics;
  DeadlockResolverDeadlockMetrics deadlock_resolver_deadlock_metrics;
  LogManagerLogs log_manager_logs;
  OrderingOverheadMetrics ord_overhead_metrics;
  ForwSequLatencyMetrics forw_sequ_latency_metrics;
  ClockSyncMetrics clock_sync_metrics;
  BatchMetrics forwarder_batch_metrics;
  BatchMetrics sequencer_batch_metrics;
  BatchMetrics mhorderer_batch_metrics;
  TxnTimestampMetrics txn_timestamp_metrics;
  GenericMetrics generic_metrics;
  ClientEpochMetrics client_epoch_metrics;
  TxnLockMetrics txn_lock_metrics;
  TxnLockArrivalMetrics txn_lock_arrival_metrics;
  LogProfilingMetrics log_profiling_metrics;
  ActiveRedirectsMetrics active_redirects_metrics;
  StubGoodputMetrics stub_goodput_metrics;
  StubQueueMetrics stub_queue_metrics;
  TigaSpeculationMetrics tiga_speculation_metrics;
};

/**
 *  MetricsRepository
 */

MetricsRepository::MetricsRepository(const ConfigurationPtr& config) : config_(config) { Reset(); }

system_clock::time_point MetricsRepository::RecordTxnEvent(TxnId txn_id, TransactionEvent event) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->txn_event_metrics.Record(txn_id, event);
}

void MetricsRepository::RecordDeadlockResolverRun(int64_t running_time, size_t unstable_graph_sz,
                                                  size_t stable_graph_sz, size_t deadlocks_resolved,
                                                  int64_t graph_update_time) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->deadlock_resolver_run_metrics.Record(running_time, unstable_graph_sz, stable_graph_sz,
                                                        deadlocks_resolved, graph_update_time);
}

void MetricsRepository::RecordDeadlockResolverDeadlock(int num_vertices,
                                                       const vector<pair<uint64_t, uint64_t>>& edges_removed,
                                                       const vector<pair<uint64_t, uint64_t>>& edges_added) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->deadlock_resolver_deadlock_metrics.Record(num_vertices, edges_removed, edges_added);
}

void MetricsRepository::RecordLogManagerEntry(uint32_t region, BatchId batch_id, TxnId txn_id, int64_t txn_timestamp,
                                              int64_t mh_depart_from_coordinator_time, int64_t mh_arrive_at_home_time,
                                              int64_t mh_enter_local_batch_time) {
  if (!config_->metric_options().logs()) {
    return;
  }
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->log_manager_logs.Record(region, batch_id, txn_id, txn_timestamp, mh_depart_from_coordinator_time,
                                           mh_arrive_at_home_time, mh_enter_local_batch_time);
}

void MetricsRepository::RecordOrderingOverhead(int64_t ordering_overhead) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->ord_overhead_metrics.Record(ordering_overhead);
}

void MetricsRepository::RecordForwSequLatency(uint32_t region, int64_t src_time, int64_t dst_time,
                                              int64_t src_recv_time, int64_t avg_time) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->forw_sequ_latency_metrics.Record(region, src_time, dst_time, src_recv_time, avg_time);
}

void MetricsRepository::RecordClockSync(uint32_t dst, int64_t src_time, int64_t dst_time, int64_t src_recv_time,
                                        int64_t local_slog_time, int64_t avg_latency, int64_t new_offset) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->clock_sync_metrics.Record(dst, src_time, dst_time, src_recv_time, local_slog_time, avg_latency,
                                             new_offset);
}

void MetricsRepository::RecordForwarderBatch(size_t batch_size, int64_t batch_duration) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->forwarder_batch_metrics.Record(0, batch_size, batch_duration);
}

void MetricsRepository::RecordSequencerBatch(BatchId batch_id, size_t batch_size, int64_t batch_duration) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->sequencer_batch_metrics.Record(batch_id, batch_size, batch_duration);
}

void MetricsRepository::RecordMHOrdererBatch(BatchId batch_id, size_t batch_size, int64_t batch_duration) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->mhorderer_batch_metrics.Record(batch_id, batch_size, batch_duration);
}

void MetricsRepository::RecordTxnTimestamp(TxnId txn_id, uint32_t from, int64_t txn_timestamp, int64_t server_time) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->txn_timestamp_metrics.Record(txn_id, from, txn_timestamp, server_time);
}

void MetricsRepository::RecordClientEpoch(uint64_t stub_id, uint64_t time, int inserted_requests, int sent_requests,
                                          int sent_noops, int sent_txns, uint64_t slots_requested,
                                          uint64_t current_slots, bool sh_stub) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->client_epoch_metrics.Record(stub_id, time, inserted_requests, sent_requests, sent_noops, sent_txns,
                                               slots_requested, current_slots, sh_stub);
}

void MetricsRepository::RecordGeneric(int type, int64_t time, int64_t data) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->generic_metrics.Record(type, time, data);
}

void MetricsRepository::RecordBlockingTxns(slog::TxnId txn_id, std::string holder_txns, std::string queue_txns) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->txn_lock_metrics.Record(txn_id, holder_txns, queue_txns);
}

void MetricsRepository::RecordUnlockingTxns(slog::TxnId txn_id, uint64_t got_request, uint64_t unlocked_all) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->txn_lock_arrival_metrics.Record(txn_id, got_request, unlocked_all);
}

void MetricsRepository::RecordLogProfiling(MachineId machine_id, BatchId batch_id, TxnId txn_id, uint64_t gsn,
                                           uint64_t insertion_timestamp, uint64_t waiting_time,
                                           uint64_t processing_time, uint64_t gsn_at_insertion) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->log_profiling_metrics.Record(machine_id, batch_id, txn_id, gsn, insertion_timestamp, waiting_time,
                                                processing_time, gsn_at_insertion);
}

void MetricsRepository::RecordActiveRedirect(uint64_t worker_id, uint64_t redirect_change_time,
                                             uint64_t active_redirects) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->active_redirects_metrics.Record(worker_id, redirect_change_time, active_redirects);
}

void MetricsRepository::RecordStubGoodputSize(uint64_t stub_id, int64_t start_timestamp, int64_t end_timestamp,
                                              uint64_t goodput, uint64_t badput, uint64_t region_slot_null,
                                              uint64_t superposition_slot_null, uint64_t region_slot_used,
                                              uint64_t superposition_slot_used) {

  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->stub_goodput_metrics.Record(stub_id, start_timestamp, end_timestamp, goodput, badput,
                                               region_slot_null, superposition_slot_null, region_slot_used,
                                               superposition_slot_used);
}

void MetricsRepository::RecordStubQueue(slog::TxnId txn_id, int64_t start_timestamp, int64_t end_timestamp){

  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->stub_queue_metrics.Record(txn_id, start_timestamp, end_timestamp);
}

void MetricsRepository::RecordSpeculationSuccess(TxnId txn_id, bool successful_speculation) {
  std::lock_guard<SpinLatch> guard(latch_);
  metrics_->tiga_speculation_metrics.Record(txn_id, successful_speculation);
}

std::unique_ptr<AllMetrics> MetricsRepository::Reset() {
  auto local_region = config_->local_region();
  auto local_partition = config_->local_partition();
  std::unique_ptr<AllMetrics> new_metrics(new AllMetrics(
      {.txn_event_metrics =
           TransactionEventMetrics(config_->metric_options().txn_events_sample(), local_region, local_partition),
       .deadlock_resolver_run_metrics = DeadlockResolverRunMetrics(
           config_->metric_options().deadlock_resolver_runs_sample(), local_region, local_partition),
       .deadlock_resolver_deadlock_metrics = DeadlockResolverDeadlockMetrics(
           config_->metric_options().deadlock_resolver_deadlocks_sample(),
           config_->metric_options().deadlock_resolver_deadlock_details(), local_region, local_partition),
       .log_manager_logs = LogManagerLogs(),
       .ord_overhead_metrics = OrderingOverheadMetrics(config_->metric_options().ordering_overhead_sample()),
       .forw_sequ_latency_metrics = ForwSequLatencyMetrics(config_->metric_options().forw_sequ_latency_sample()),
       .clock_sync_metrics = ClockSyncMetrics(config_->metric_options().clock_sync_sample()),
       .forwarder_batch_metrics = BatchMetrics(config_->metric_options().forwarder_batch_sample()),
       .sequencer_batch_metrics = BatchMetrics(config_->metric_options().sequencer_batch_sample()),
       .mhorderer_batch_metrics = BatchMetrics(config_->metric_options().mhorderer_batch_sample()),
       .txn_timestamp_metrics = TxnTimestampMetrics(config_->metric_options().txn_timestamp_sample()),
       .generic_metrics = GenericMetrics(config_->metric_options().generic_sample(), local_region, local_partition),
       .client_epoch_metrics = ClientEpochMetrics(config_->metric_options().client_epochs_sample()),
       .txn_lock_metrics = TxnLockMetrics(config_->metric_options().txn_locks_sample()),
       .txn_lock_arrival_metrics = TxnLockArrivalMetrics(config_->metric_options().txn_locks_sample()),
       .log_profiling_metrics = LogProfilingMetrics(config_->metric_options().logs() ? 100 : 0),
       .active_redirects_metrics = ActiveRedirectsMetrics(config_->metric_options().active_redirect_sample()),
       .stub_goodput_metrics = StubGoodputMetrics(config_->metric_options().stub_goodput_sample()),
      .stub_queue_metrics = StubQueueMetrics(config_->metric_options().stub_goodput_sample()),
      .tiga_speculation_metrics = TigaSpeculationMetrics(100)}));

  std::lock_guard<SpinLatch> guard(latch_);
  metrics_.swap(new_metrics);

  return new_metrics;
}

thread_local std::shared_ptr<MetricsRepository> per_thread_metrics_repo;

/**
 *  MetricsRepositoryManager
 */

MetricsRepositoryManager::MetricsRepositoryManager(const std::string& config_name, const ConfigurationPtr& config)
    : config_name_(config_name), config_(config) {}

void MetricsRepositoryManager::RegisterCurrentThread() {
  std::lock_guard<std::mutex> guard(mut_);
  const auto thread_id = std::this_thread::get_id();
  auto ins = metrics_repos_.try_emplace(thread_id, config_, new MetricsRepository(config_));
  per_thread_metrics_repo = ins.first->second;
}

void MetricsRepositoryManager::AggregateAndFlushToDisk(const std::string& dir) {
  // Aggregate metrics
  list<TransactionEventMetrics::Data> txn_events_data;
  list<DeadlockResolverRunMetrics::Data> deadlock_resolver_run_data;
  list<DeadlockResolverDeadlockMetrics::Data> deadlock_resolver_deadlock_data;
  vector<LogManagerLogs::Data> global_log;
  list<OrderingOverheadMetrics::Data> ord_overhead_data;
  list<ForwSequLatencyMetrics::Data> forw_sequ_latency_data;
  list<ClockSyncMetrics::Data> clock_sync_data;
  list<BatchMetrics::Data> forwarder_batch_data, sequencer_batch_data, mhorderer_batch_data;
  list<TxnTimestampMetrics::Data> txn_timestamp_data;
  list<ClientEpochMetrics::Data> client_epochs_data;
  list<GenericMetrics::Data> generic_data;
  list<TxnLockMetrics::Data> txn_locks_data;
  list<TxnLockArrivalMetrics::Data> txn_locks_arrival_data;
  list<LogProfilingMetrics::Data> log_profiling_data;
  list<ActiveRedirectsMetrics::Data> active_redirects_data;
  list<StubGoodputMetrics::Data> stub_goodput_data;
  list<StubQueueMetrics::Data> stub_queue_data;
  list<TigaSpeculationMetrics::Data> tiga_speculation_data;
  {
    std::lock_guard<std::mutex> guard(mut_);
    for (auto& kv : metrics_repos_) {
      auto metrics = kv.second->Reset();
      txn_events_data.splice(txn_events_data.end(), metrics->txn_event_metrics.data());
      deadlock_resolver_run_data.splice(deadlock_resolver_run_data.end(),
                                        metrics->deadlock_resolver_run_metrics.data());
      deadlock_resolver_deadlock_data.splice(deadlock_resolver_deadlock_data.end(),
                                             metrics->deadlock_resolver_deadlock_metrics.data());
      // There is only one thread with local logs and global log data so there is no need to splice
      if (!metrics->log_manager_logs.global_log().empty()) {
        global_log = metrics->log_manager_logs.global_log();
      }
      ord_overhead_data.splice(ord_overhead_data.end(), metrics->ord_overhead_metrics.data());
      forw_sequ_latency_data.splice(forw_sequ_latency_data.end(), metrics->forw_sequ_latency_metrics.data());
      clock_sync_data.splice(clock_sync_data.end(), metrics->clock_sync_metrics.data());
      forwarder_batch_data.splice(forwarder_batch_data.end(), metrics->forwarder_batch_metrics.data());
      sequencer_batch_data.splice(sequencer_batch_data.end(), metrics->sequencer_batch_metrics.data());
      mhorderer_batch_data.splice(mhorderer_batch_data.end(), metrics->mhorderer_batch_metrics.data());
      txn_timestamp_data.splice(txn_timestamp_data.end(), metrics->txn_timestamp_metrics.data());
      client_epochs_data.splice(client_epochs_data.end(), metrics->client_epoch_metrics.data());
      generic_data.splice(generic_data.end(), metrics->generic_metrics.data());
      txn_locks_data.splice(txn_locks_data.end(), metrics->txn_lock_metrics.data());
      txn_locks_arrival_data.splice(txn_locks_arrival_data.end(), metrics->txn_lock_arrival_metrics.data());
      log_profiling_data.splice(log_profiling_data.end(), metrics->log_profiling_metrics.data());
      active_redirects_data.splice(active_redirects_data.end(), metrics->active_redirects_metrics.data());
      stub_goodput_data.splice(stub_goodput_data.end(), metrics->stub_goodput_metrics.data());
      stub_queue_data.splice(stub_queue_data.end(), metrics->stub_queue_metrics.data());
      tiga_speculation_data.splice(tiga_speculation_data.end(), metrics->tiga_speculation_metrics.data());
    }
  }

  // Write metrics to disk
  try {
    CSVWriter metadata_csv(dir + "/metadata.csv", {"version", "config_name"});
    metadata_csv << SLOG_VERSION << config_name_;

    TransactionEventMetrics::WriteToDisk(dir, txn_events_data);
    DeadlockResolverRunMetrics::WriteToDisk(dir, deadlock_resolver_run_data);
    DeadlockResolverDeadlockMetrics::WriteToDisk(dir, deadlock_resolver_deadlock_data,
                                                 config_->metric_options().deadlock_resolver_deadlock_details());
    LogManagerLogs::WriteToDisk(dir, global_log);
    ForwSequLatencyMetrics::WriteToDisk(dir, forw_sequ_latency_data);
    ClockSyncMetrics::WriteToDisk(dir, clock_sync_data);
    BatchMetrics::WriteToDisk(dir + "/forwarder_batch.csv", forwarder_batch_data);
    BatchMetrics::WriteToDisk(dir + "/sequencer_batch.csv", sequencer_batch_data);
    BatchMetrics::WriteToDisk(dir + "/mhorderer_batch.csv", mhorderer_batch_data);
    TxnTimestampMetrics::WriteToDisk(dir, txn_timestamp_data);
    GenericMetrics::WriteToDisk(dir, generic_data);
    ClientEpochMetrics::WriteToDisk(dir + "/client_epochs.csv", client_epochs_data);
    OrderingOverheadMetrics::WriteToDisk(dir, ord_overhead_data);
    TxnLockMetrics::WriteToDisk(dir + "/txn_locks.csv", txn_locks_data);
    TxnLockArrivalMetrics::WriteToDisk(dir + "/txn_locks_arrival.csv", txn_locks_arrival_data);
    LogProfilingMetrics::WriteToDisk(dir + "/log_profiling.csv", log_profiling_data);
    ActiveRedirectsMetrics::WriteToDisk(dir + "/active_redirect.csv", active_redirects_data);
    StubGoodputMetrics::WriteToDisk(dir + "/stub_goodput.csv", stub_goodput_data);
    StubQueueMetrics::WriteToDisk(dir + "/stub_queue.csv", stub_queue_data);
    TigaSpeculationMetrics::WriteToDisk(dir + "/speculation_results.csv", tiga_speculation_data);

    LOG(INFO) << "Metrics written to: \"" << dir << "/\"";
  } catch (std::runtime_error& e) {
    LOG(ERROR) << e.what();
  }
}

void MetricsRepositoryManager::FlushClientEpochs(const std::string& dir) {
  std::lock_guard<std::mutex> guard(mut_);
  list<ClientEpochMetrics::Data> client_epochs_data;

  for (auto& kv : metrics_repos_) {
    auto metrics = kv.second->Reset();
    client_epochs_data.splice(client_epochs_data.end(), metrics->client_epoch_metrics.data());
    LOG(INFO) << "Length of flush: " << client_epochs_data.size();
  }
  try {
    ClientEpochMetrics::WriteToDisk(dir + "/client_epochs.csv", client_epochs_data);
  } catch (std::runtime_error& e) {
    LOG(ERROR) << e.what();
  }
}
/**
 * Initialization
 */

uint32_t gLocalMachineId = 0;
uint64_t gEnabledEvents = 0;
uint32_t gHome = 0;

void InitializeRecording(const ConfigurationPtr& config) {
  gLocalMachineId = config->local_machine_id();
  gHome = config->local_region();
  auto events = config->enabled_events();
  for (auto e : events) {
    if (e == TransactionEvent::ALL) {
      gEnabledEvents = ~0;
      return;
    }
    gEnabledEvents |= (1 << e);
  }
}

}  // namespace slog