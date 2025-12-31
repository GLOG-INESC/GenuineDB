#pragma once

#include <glog/logging.h>

#include <optional>
#include <unordered_set>
#include <vector>

#include "general_test_utils.h"

#include "common/configuration.h"
#include "connection/broker.h"
#include "connection/poller.h"
#include "connection/sender.h"
#include "connection/zmq_utils.h"
#include "module/base/module.h"
#include "module/scheduler_components/txn_holder.h"
#include "proto/internal.pb.h"
#include "storage/mem_only_storage.h"
#include "storage/metadata_initializer.h"
#include "workload/workload.h"
#include "common/rate_limiter.h"

using std::pair;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::chrono::milliseconds;

namespace slog {

using ConfigVec = std::vector<ConfigurationPtr>;



ConfigVec MakeTestConfigurations(string&& prefix, int num_regions, int num_replicas, int num_partitions,
                                 internal::Configuration common_config = {}, bool local_sync_rep = false);

Transaction* MakeTestTransaction(const ConfigurationPtr& config, TxnId id, const std::vector<KeyMetadata>& keys,
                                 const std::vector<std::vector<std::string>> code = {{}},
                                 std::optional<int> remaster = std::nullopt, MachineId coordinator = 0);

TxnHolder MakeTestTxnHolder(const ConfigurationPtr& config, TxnId id, const std::vector<KeyMetadata>& keys,
                            const std::vector<std::vector<std::string>> code = {{}},
                            std::optional<int> remaster = std::nullopt);


using ModuleRunnerPtr = unique_ptr<ModuleRunner>;

/**
 * This is a fake SLOG system where we can only add a subset
 * of modules to test them in isolation.
 */
class TestSlog : public TestSystem {
 public:
  TestSlog(const ConfigurationPtr& config, bool init = false);
  void AddServerAndClient();
  void AddForwarder();
  void AddSimpleMetadataForwarder();
  void AddSequencer();
  void AddLogManagers();
  void AddScheduler();
  void AddLocalPaxos();
  void AddGlobalPaxos();
  void AddMultiHomeOrderer();
  void StartSlogInNewThreads();

  void SendTxn(Transaction* txn);
  Transaction RecvTxnResult(bool timeout = false);

 protected:
  ModuleRunnerPtr sequencer_;
  zmq::context_t client_context_;
  zmq::socket_t client_socket_;
  std::vector<ModuleRunnerPtr> log_managers_;
  ModuleRunnerPtr server_;
  ModuleRunnerPtr forwarder_;

 private:
  ModuleRunnerPtr scheduler_;
  ModuleRunnerPtr local_paxos_;
  ModuleRunnerPtr global_paxos_;
  ModuleRunnerPtr multi_home_orderer_;

};


}// namespace slog