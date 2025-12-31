#include "test/test_utils.h"

#include <glog/logging.h>

#include <random>
#include <set>

#include "common/proto_utils.h"
#include "connection/zmq_utils.h"
#include "module/consensus.h"
#include "module/forwarder.h"
#include "module/log_manager.h"
#include "module/multi_home_orderer.h"
#include "module/scheduler.h"
#include "module/sequencer.h"
#include "module/server.h"
#include "proto/api.pb.h"
#include "execution/tpcc/metadata_initializer.h"

using std::make_shared;
using std::to_string;

namespace slog {

using std::chrono::milliseconds;

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

ConfigVec MakeTestConfigurations(string&& prefix, int num_regions, int num_replicas, int num_partitions,
                                 internal::Configuration common_config, bool local_sync_rep) {
  int num_machines = num_regions * num_partitions;
  string addr = "/tmp/test_" + prefix;

  common_config.set_protocol("ipc");
  common_config.add_broker_ports(0);
  common_config.add_broker_ports(1);
  common_config.set_forwarder_port(2);
  common_config.set_sequencer_port(3);
  common_config.set_multi_home_orderer_port(4);
  common_config.set_ddr_resolver_port(5);

  common_config.set_num_partitions(num_partitions);

  if (!common_config.has_simple_partitioning() && !common_config.has_tpcc_partitioning() ){
    common_config.mutable_hash_partitioning()->set_partition_key_num_bytes(1);

  }




  //common_config.mutable_hash_partitioning()->set_partition_key_num_bytes(1);
  common_config.set_sequencer_batch_duration(1);
  common_config.set_forwarder_batch_duration(1);

  if (common_config.execution_type() == 0){
    common_config.set_execution_type(internal::ExecutionType::KEY_VALUE);
  }

  int counter = 0;
  for (int reg = 0; reg < num_regions; reg++) {
    auto region = common_config.add_regions();
    for (int rep = 0; rep < num_replicas; rep++) {
      for (int p = 0; p < num_partitions; p++) {
        region->add_addresses(addr + to_string(counter++));
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
        string local_addr = addr + to_string(counter++);
        configs.push_back(std::make_shared<Configuration>(common_config, local_addr));
      }
    }
  }

  return configs;
}


Transaction* MakeTestTransaction(const ConfigurationPtr& config, TxnId id, const std::vector<KeyMetadata>& keys,
                                 const std::vector<std::vector<std::string>> code, std::optional<int> remaster,
                                 MachineId coordinator) {
  auto txn = MakeTransaction(keys, code, remaster, coordinator);
  txn->mutable_internal()->set_id(id);

  PopulateInvolvedPartitions(Sharder::MakeSharder(config), *txn);

  return txn;
}

TxnHolder MakeTestTxnHolder(const ConfigurationPtr& config, TxnId id, const std::vector<KeyMetadata>& keys,
                            const std::vector<std::vector<std::string>> code, std::optional<int> remaster) {
  auto txn = MakeTestTransaction(config, id, keys, code, remaster);

  auto sharder = Sharder::MakeSharder(config);
  vector<Transaction*> lo_txns;
  for (int i = 0; i < txn->internal().involved_regions_size(); ++i) {
    auto lo = GenerateLockOnlyTxn(txn, txn->internal().involved_regions(i));
#ifdef PARTIAL_REP
    auto partitioned_lo = GeneratePartitionedTxn(sharder, lo, txn->internal().involved_regions(i), sharder->local_partition(), true);
#else
    auto partitioned_lo = GeneratePartitionedTxn(sharder, lo, sharder->local_partition(), true);
#endif
    if (partitioned_lo != nullptr) {
      lo_txns.push_back(partitioned_lo);
    }
  }
  delete txn;

  CHECK(!lo_txns.empty());

  TxnHolder holder(config, lo_txns[0]);
  for (size_t i = 1; i < lo_txns.size(); ++i) {
    holder.AddLockOnlyTxn(lo_txns[i]);
  }
  return holder;
}



TestSlog::TestSlog(const ConfigurationPtr& config, bool init) : TestSystem(config, init),
      client_context_(1) {
  client_context_.set(zmq::ctxopt::blocky, false);
  client_socket_ = zmq::socket_t(client_context_, ZMQ_DEALER);

}

void TestSlog::AddServerAndClient() { server_ = MakeRunnerFor<Server>(broker_, nullptr, kTestModuleTimeout); }

void TestSlog::AddSimpleMetadataForwarder() {
  auto config = broker_->config();
  if (config->proto_config().partitioning_case() == internal::Configuration::kTpccPartitioning){
    metadata_initializer_ = make_shared<tpcc::TPCCMetadataInitializer>(config->num_regions(), config->num_partitions(), config->local_region());
  } else {
    metadata_initializer_ = make_shared<SimpleMetadataInitializer>(config->num_regions(), config->num_partitions());
  }
  forwarder_ = MakeRunnerFor<Forwarder>(broker_->context(), broker_->config(), storage_, metadata_initializer_, nullptr,
                                        kTestModuleTimeout);
}
void TestSlog::AddForwarder() {

  metadata_initializer_ = std::make_shared<ConstantMetadataInitializer>(0);
  forwarder_ = MakeRunnerFor<Forwarder>(broker_->context(), broker_->config(), storage_, metadata_initializer_, nullptr,
                                        kTestModuleTimeout);
}

void TestSlog::AddSequencer() {
  sequencer_ = MakeRunnerFor<Sequencer>(broker_->context(), broker_->config(), nullptr, kTestModuleTimeout);
}

void TestSlog::AddLogManagers() {
  auto num_log_managers = broker_->config()->num_log_managers();
  for (int i = 0; i < num_log_managers; i++) {
    std::vector<RegionId> regions;
    for (int r = 0; r < broker_->config()->num_regions(); r++) {
      if (r % num_log_managers == i) {
        regions.push_back(r);
      }
    }
    log_managers_.push_back(MakeRunnerFor<slog::LogManager>(i, regions, broker_, nullptr, kTestModuleTimeout));
  }
}

void TestSlog::AddScheduler() { scheduler_ = MakeRunnerFor<Scheduler>(broker_, storage_, nullptr, kTestModuleTimeout); }

void TestSlog::AddLocalPaxos() { local_paxos_ = MakeRunnerFor<LocalPaxos>(broker_, kTestModuleTimeout); }

void TestSlog::AddGlobalPaxos() { global_paxos_ = MakeRunnerFor<GlobalPaxos>(broker_, kTestModuleTimeout); }

void TestSlog::AddMultiHomeOrderer() {
  multi_home_orderer_ = MakeRunnerFor<MultiHomeOrderer>(broker_->context(), broker_->config(), nullptr, kTestModuleTimeout);
}

void TestSlog::StartSlogInNewThreads() {
  StartInNewThreads();

  if (server_) {
    server_->StartInNewThread();
    string endpoint = "tcp://localhost:" + to_string(config_->server_port());
    client_socket_.connect(endpoint);
  }
  if (forwarder_) {
    forwarder_->StartInNewThread();
  }
  if (sequencer_) {
    sequencer_->StartInNewThread();
  }
  if (!log_managers_.empty()) {
    for (auto& lm : log_managers_) {
      lm->StartInNewThread();
    }
  }
  if (scheduler_) {
    scheduler_->StartInNewThread();
  }
  if (local_paxos_) {
    local_paxos_->StartInNewThread();
  }
  if (global_paxos_) {
    global_paxos_->StartInNewThread();
  }
  if (multi_home_orderer_) {
    multi_home_orderer_->StartInNewThread();
  }

}


void TestSlog::SendTxn(Transaction* txn) {
  CHECK(server_ != nullptr) << "TestSlog does not have a server";
  api::Request request;
  auto txn_req = request.mutable_txn();
  txn_req->set_allocated_txn(txn);
  SendSerializedProtoWithEmptyDelim(client_socket_, request);
}

Transaction TestSlog::RecvTxnResult(bool timeout) {
  api::Response res;
  if (!RecvDeserializedProtoWithEmptyDelim(client_socket_, res, timeout)) {
    LOG(FATAL) << "Malformed response to client transaction.";
    return Transaction();
  }
  const auto& txn = res.txn().txn();
  return txn;
}

}  // namespace slog
