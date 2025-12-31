#pragma once

#include <chrono>
#include <string>
#include <vector>

#include "common/types.h"
#include "proto/configuration.pb.h"
#include "proto/internal.pb.h"
#include "common/constants.h"

namespace slog {

class Configuration;

using ConfigurationPtr = std::shared_ptr<const Configuration>;

class Configuration {
 public:
  static ConfigurationPtr FromFile(const std::string& file_path, const std::string& local_address = "");

  Configuration(const internal::Configuration& config, const std::string& local_address);

  const internal::Configuration& proto_config() const;
  const std::string& protocol() const;
  const std::string& address(RegionId region, ReplicaId replica, PartitionId partition) const;
  const std::string& address(MachineId machine_id) const;
  uint32_t broker_ports(int i) const;
  uint32_t broker_ports_size() const;
  uint32_t server_port() const;
  uint32_t forwarder_port() const;
  uint32_t sequencer_port() const;
  uint32_t clock_synchronizer_port() const;
  uint32_t glog_port() const;
  uint32_t order_port() const;
  uint32_t mh_order_port() const;
  uint32_t ddr_port() const;
  uint32_t stub_port() const;
  int num_regions() const;
  int num_replicas(RegionId reg) const;
  int num_partitions() const;
  int num_workers() const;
  int num_log_managers() const;
  std::vector<MachineId> all_machine_ids() const;
  std::chrono::milliseconds mh_orderer_batch_duration() const;
  std::chrono::milliseconds forwarder_batch_duration() const;
  std::chrono::milliseconds sequencer_batch_duration() const;
  int sequencer_batch_size() const;
  bool sequencer_rrr() const;
  uint32_t replication_factor() const;
  bool local_sync_replication() const;

  const std::string& local_address() const;
  RegionId local_region() const;
  ReplicaId local_replica() const;
  PartitionId local_partition() const;
  MachineId local_machine_id() const;

  RegionId leader_region_for_multi_home_ordering() const;
  PartitionId leader_partition_for_multi_home_ordering() const;

  uint32_t replication_delay_pct() const;
  uint32_t replication_delay_amount_ms() const;

  std::vector<TransactionEvent> enabled_events() const;
  bool bypass_mh_orderer() const;
  std::chrono::milliseconds ddr_interval() const;
  std::vector<int> cpu_pinnings(ModuleId module) const;
  internal::ExecutionType execution_type() const;
  const std::vector<uint32_t>& replication_order() const;
  bool synchronized_batching() const;
  const internal::MetricOptions& metric_options() const;
  std::chrono::milliseconds fs_latency_interval() const;
  std::chrono::milliseconds clock_sync_interval() const;
  int64_t timestamp_buffer_us() const;
  uint32_t avg_latency_window_size() const;
  bool shrink_mh_orderer() const;
  std::vector<int> distance_ranking_from(RegionId region_id) const;

  int broker_rcvbuf() const;
  int long_sender_sndbuf() const;
  int tps_limit() const;

  // Ziplog configurations
  std::chrono::milliseconds epoch_duration() const;
  std::string order_address() const;
  MachineId order_machine_id() const;

  uint64_t start_slots() const;

  double smoothing() const;
  uint64_t measurements() const;
  double increment() const;
  double noops() const;
  unsigned long min_slots() const;
  internal::LogType log_type() const;
  bool separate_types() const;
  unsigned long min_slots_sh() const;
  unsigned long rounding_factor() const;
  unsigned long num_stubs() const;
  bool has_simulated_delays() const;
  bool shrink_stubs() const;

  unsigned long txn_issuing_delay() const;
  // Returns the array of network delays for a given region
  std::array<double, MAX_NUM_REGIONS> network_delays(RegionId region) const;
 private:
  internal::Configuration config_;
  std::string local_address_;
  int local_region_;
  int local_replica_;
  int local_partition_;
  MachineId local_machine_id_;

  std::unordered_map<MachineId, std::string> all_addresses_;
  std::vector<MachineId> all_machine_ids_;
  std::vector<uint32_t> replication_order_;
  std::array< std::array<double, MAX_NUM_REGIONS>, MAX_NUM_REGIONS> simulated_delays_;
  bool has_simulated_delays_ = false;
};

}  // namespace slog