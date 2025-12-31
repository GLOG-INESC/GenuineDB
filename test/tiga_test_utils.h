#pragma once

#include <glog/logging.h>

#include <optional>
#include <unordered_set>
#include <vector>

#include "common/configuration.h"
#include "connection/broker.h"
#include "common/proto_utils.h"
#include "storage/metadata_initializer.h"
#include "storage/speculative_mem_only_storage.h"
#include "module/server.h"
#include "workload/workload.h"
#include "general_test_utils.h"

using std::pair;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::chrono::milliseconds;

namespace tiga {

using slog::ConfigurationPtr;
using slog::internal::Configuration;
using slog::Transaction;
using slog::TxnId;

using slog::MachineId;
using slog::KeyMetadata;
using slog::ModuleRunner;
using slog::Broker;
using slog::MetadataInitializer;
using slog::MemOnlyStorage;
using slog::Channel;

using ConfigVec = std::vector<ConfigurationPtr>;

using ModuleRunnerPtr = unique_ptr<ModuleRunner>;

ConfigVec MakeTigaTestConfigurations(string&& prefix, int num_regions, int num_replicas, int num_partitions,
                                      Configuration common_config = {}, bool local_sync_rep = false);
/**
 * This is a fake Tiga system where we can only add a subset
 * of modules to test them in isolation.
 */
class TestTiga : public slog::TestSystem {
 public:
  TestTiga(const ConfigurationPtr& config, bool debug_flag = false, bool init = false);
  void AddServerAndClient();
  void AddCoordinator();
  void AddAcceptor();
  void AddScheduler();
  void AddWorkers();
  void StartTigaInNewThreads();
  void SendTxn(Transaction* txn);
 Transaction RecvTxnResult(bool timeout = false);
  Transaction RecvTxnResultTimeout();
  void Data(slog::Key&& key, slog::Record&& record);

 private:
  ModuleRunnerPtr server_;
  ModuleRunnerPtr coordinator_;
  ModuleRunnerPtr acceptor_;
  ModuleRunnerPtr scheduler_;
  std::vector<ModuleRunnerPtr> workers_;

  zmq::context_t client_context_;
  zmq::socket_t client_socket_;
  shared_ptr<slog::SpeculativeMemOnlyStorage> spec_storage_;

  bool test_coordinator_flag_;

};


}// namespace slog