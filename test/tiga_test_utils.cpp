#include "test/tiga_test_utils.h"

#include <glog/logging.h>

#include "module/base/module.h"
#include "module/tiga/acceptor.h"
#include "module/tiga/coordinator.h"
#include "module/txn_generator.h"
#include "module/tiga/scheduler.h"

#include "storage/init.h"

namespace {

std::set<uint32_t> used_ports;
std::mt19937 rng(std::random_device{}());

uint32_t NextUnusedPort() {
  std::uniform_int_distribution<> dis(10000, 30000);
  uint32_t port;
  do {
    port = dis(rng);
  } while (used_ports.find(port) != used_ports.end());
  used_ports.insert(port);
  return port;
}

}  // namespace

namespace tiga {

using slog::kTestModuleTimeout;
using slog::MakeRunnerFor;

ConfigVec MakeTigaTestConfigurations(string&& prefix, int num_regions, int num_replicas, int num_partitions,
                                      Configuration common_config, bool local_sync_rep) {
  int num_machines = num_regions * num_partitions;
  string addr = "/tmp/test_" + prefix;

  common_config.set_protocol("ipc");
  common_config.add_broker_ports(0);
  common_config.add_broker_ports(1);
  common_config.set_forwarder_port(2);
  common_config.set_sequencer_port(3);

  common_config.set_num_partitions(num_partitions);
  /*
  if (common_config.mutable_simple_partitioning()->num_records() == 0) {
    common_config.mutable_hash_partitioning()->set_partition_key_num_bytes(1);
  }
  */

  if (common_config.execution_type() == 0) {
    common_config.set_execution_type(slog::internal::ExecutionType::KEY_VALUE);
  }

  int counter = 0;
  for (int reg = 0; reg < num_regions; reg++) {
    auto region = common_config.add_regions();
    for (int rep = 0; rep < num_replicas; rep++) {
      for (int p = 0; p < num_partitions; p++) {
        region->add_addresses(addr + std::to_string(counter++));
        region->set_num_replicas(num_replicas);
        region->set_sync_replication(local_sync_rep);
      }
    }
  }

  ConfigVec configs;
  configs.reserve(num_machines);

  counter = 0;
  for (int reg = 0; reg < num_regions; reg++) {
    for (int rep = 0; rep < num_replicas; rep++) {
      for (int part = 0; part < num_partitions; part++) {
        // Generate different server ports because tests
        // run on the same machine
        common_config.set_server_port(NextUnusedPort());
        string local_addr = addr + std::to_string(counter++);
        configs.push_back(std::make_shared<slog::Configuration>(common_config, local_addr));
      }
    }
  }

  return configs;
}

TestTiga::TestTiga(const slog::ConfigurationPtr& config, bool debug_flag, bool init)
    : slog::TestSystem(config, false), client_context_(1), spec_storage_(new SpeculativeMemOnlyStorage()), test_coordinator_flag_(debug_flag) {
  client_context_.set(zmq::ctxopt::blocky, false);
  client_socket_ = zmq::socket_t(client_context_, ZMQ_DEALER);

  if (config_->proto_config().partitioning_case() == Configuration::kTpccPartitioning){
    metadata_initializer_ = std::make_shared<slog::tpcc::TPCCMetadataInitializer>(config_->num_regions(), config_->num_partitions(), config_->local_region());
  } else {
    metadata_initializer_ = std::make_shared<slog::SimpleMetadataInitializer>(config_->num_regions(), config_->num_partitions());
  }

  if (init) {
    auto [storage, metadata_initializer] = slog::MakeSpecStorage(config, "");
    spec_storage_ = storage;
    metadata_initializer_ = metadata_initializer;
  }

}

void TestTiga::AddServerAndClient() {
  server_ = MakeRunnerFor<slog::Server>(broker_, nullptr, kTestModuleTimeout);
  string endpoint = "tcp://localhost:" + std::to_string(config_->server_port());
  client_socket_.connect(endpoint);
}

void TestTiga::AddCoordinator() {
  coordinator_ = MakeRunnerFor<Coordinator>(broker_->context(), broker_->config(), metadata_initializer_, nullptr, kTestModuleTimeout, test_coordinator_flag_);
}

void TestTiga::AddAcceptor() {
  acceptor_ = MakeRunnerFor<Acceptor>(broker_->context(), broker_->config(), nullptr, kTestModuleTimeout, test_coordinator_flag_);
}

void TestTiga::AddScheduler() {
  scheduler_ = MakeRunnerFor<Scheduler>(broker_, spec_storage_, nullptr, kTestModuleTimeout, test_coordinator_flag_);
}

void TestTiga::AddWorkers() {
  auto num_workers = broker_->config()->num_workers();

  for (int i = 0; i < num_workers; i++) {
    workers_.push_back(MakeRunnerFor<Worker>(i, broker_, spec_storage_, nullptr, kTestModuleTimeout));
  }
}

void TestTiga::StartTigaInNewThreads() {
  StartInNewThreads();

  if (server_) {
    server_->StartInNewThread();
  }

  if (coordinator_) {
    coordinator_->StartInNewThread();
  }

  if (acceptor_) {
    acceptor_->StartInNewThread();
  }

  if (scheduler_) {
    scheduler_->StartInNewThread();
    CHECK(workers_.size() == 0) << "Tiga Test cant handle worker modules if scheduler model is active";
  }

  if (workers_.size() > 0) {
    // Only initialize workers if the scheduler was inactive
    for (auto & worker : workers_) {
      worker->StartInNewThread();
    }
    CHECK(!scheduler_) << "Tiga Test cant handle worker modules if scheduler model is active";
  }

}

void TestTiga::SendTxn(Transaction* txn) {
  CHECK(server_ != nullptr) << "TestSlog does not have a server";
  slog::api::Request request;
  auto txn_req = request.mutable_txn();
  txn_req->set_allocated_txn(txn);
  slog::SendSerializedProtoWithEmptyDelim(client_socket_, request);
}

Transaction TestTiga::RecvTxnResult(bool timeout) {
  slog::api::Response res;
  if (!slog::RecvDeserializedProtoWithEmptyDelim(client_socket_, res, timeout)) {
    LOG(FATAL) << "Malformed response to client transaction.";
    return Transaction();
  }
  const auto& txn = res.txn().txn();
  return txn;
}

Transaction TestTiga::RecvTxnResultTimeout() {
  slog::api::Response res;
  if (!slog::RecvDeserializedProtoPoolRecv(client_socket_, res, true)) {
    return Transaction();
  }
  const auto& txn = res.txn().txn();
  return txn;
}

void TestTiga::Data(slog::Key&& key, slog::Record&& record) {
  CHECK(sharder_->is_local_key(key)) << "Key \"" << key << "\" belongs to partition "
                                   << sharder_->compute_partition(key);

  spec_storage_->DefinitiveWrite(key, record);
}

}  // namespace janus
