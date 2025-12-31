//
// Created by jrsoares on 31-03-2025.
//

#include "glog_test_utils.h"

#include "module/glog/glog_order.h"
#include "module/glog/glog_stub.h"
#include "module/glog/glog.h"
#include "module/forwarder.h"

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

}


namespace glog {

ConfigVec MakeGlogTestConfigurations(string&& prefix, int num_regions, int num_replicas, int num_partitions,
                                     internal::Configuration common_config, bool local_sync_rep) {
  int num_machines = num_regions * num_partitions;
  string addr = "/tmp/test_" + prefix;

  common_config.set_protocol("ipc");
  common_config.add_broker_ports(0);
  common_config.add_broker_ports(1);
  common_config.set_forwarder_port(2);
  common_config.set_sequencer_port(3);
  common_config.set_glog_port(4);
  common_config.set_order_port(5);
  common_config.set_glog_stub_port(6);

  common_config.set_num_partitions(num_partitions);
  // Only activate hash if simple partitioning is not activated
  /*
  if (common_config.mutable_simple_partitioning()->num_records() == 0){
    common_config.mutable_hash_partitioning()->set_partition_key_num_bytes(1);
  }
   */

  if (common_config.execution_type() == 0){
    common_config.set_execution_type(internal::ExecutionType::KEY_VALUE);
  }

  common_config.set_sequencer_batch_duration(0);
  common_config.set_forwarder_batch_duration(0);

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
  // We assume that Ordered will always be on the last Machine
  common_config.mutable_order_address()->set_private_address(addr + to_string(counter));
  common_config.mutable_order_address()->set_public_address(addr + to_string(counter++));

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

  string order_local_addr = addr + to_string(counter++);
  common_config.set_server_port(NextUnusedPort());
  configs.push_back(std::make_shared<Configuration>(common_config, order_local_addr));


  return configs;
}

TestGlog::TestGlog(const ConfigurationPtr& config, bool init) : TestSlog(config, init){};

void TestGlog::AddOrder() {
  order_ = MakeRunnerFor<GLOGOrder>(broker_->context(), broker_->config(), nullptr, kTestModuleTimeout);
}

void TestGlog::AddGlogStubs(int num_glog_stubs, bool noop_responds) {
  for (int i = 0; i < num_glog_stubs; i++){
    glog_stubs_.push_back(MakeRunnerFor<GlogStub>(this->config_->local_region(), this->config_->local_replica(), this->config_->local_partition(),
                                                  i, broker_, nullptr, kTestModuleTimeout, noop_responds));
  }
}

void TestGlog::AddGlog() {
  glog_ = MakeRunnerFor<glog::GLOG>(0, broker_->context(), broker_->config(), nullptr, kTestModuleTimeout);
}

void TestGlog::AddGlogOutputSocket(slog::Channel channel, const std::vector<uint64_t>& tags) {
  switch (channel) {
    case kGLOGChannel: {
      zmq::socket_t outproc_socket(*broker_->context(), ZMQ_PULL);
      outproc_socket.bind(
          MakeRemoteAddress(config_->protocol(), config_->local_address(), config_->glog_port(), true));
      outproc_sockets_[channel] = std::move(outproc_socket);

      zmq::socket_t inproc_socket(*broker_->context(), ZMQ_PULL);
      inproc_socket.bind(MakeInProcChannelAddress(channel));
      inproc_sockets_.insert_or_assign(channel, std::move(inproc_socket));
      break;
    }

    case kZipOrderChannel: {
      zmq::socket_t outproc_socket(*broker_->context(), ZMQ_PULL);
      outproc_socket.bind(
          MakeRemoteAddress(config_->protocol(), config_->order_address(), config_->order_port(), true));
      outproc_sockets_[channel] = std::move(outproc_socket);

      zmq::socket_t inproc_socket(*broker_->context(), ZMQ_PULL);
      inproc_socket.bind(MakeInProcChannelAddress(channel));
      inproc_sockets_.insert_or_assign(channel, std::move(inproc_socket));
      break;
    }
    default:
      AddOutputSocket(channel, tags);
  }

}
void TestGlog::StartGlogInNewThreads(){
  StartSlogInNewThreads();
  if (order_){
    order_->StartInNewThread();
  }

  if (glog_){
    glog_->StartInNewThread();
  }

  if (!glog_stubs_.empty()){
    for (auto& stub : glog_stubs_) {
      stub->StartInNewThread();
    }
  }

}


milliseconds TestGlog::GetEpochDuration(){
  return config_->epoch_duration();
}

}
