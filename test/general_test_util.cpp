//
// Created by jrsoares on 09/10/25.
//

#include "common/proto_utils.h"
#include "execution/tpcc/load_tables.h"
#include "execution/tpcc/metadata_initializer.h"
#include "general_test_utils.h"
#include "module/scheduler_components/worker.h"
#include "module/txn_generator.h"
#include "storage/init.h"

namespace slog {

ValueEntry TxnValueEntry(const Transaction& txn, const std::string& key) {
  auto it = std::find_if(txn.keys().begin(), txn.keys().end(),
                         [&key](const KeyValueEntry& entry) { return entry.key() == key; });
  return it->value_entry();
}

TestSystem::TestSystem(const slog::ConfigurationPtr &config, bool init)
    : config_(config), sharder_(Sharder::MakeSharder(config)), storage_(new MemOnlyStorage()),
      broker_(Broker::New(config, kTestModuleTimeout)){

  if (init) {
    auto [storage, metadata_initializer] = MakeStorage(config, "");
    storage_ = storage;

    metadata_initializer_ = metadata_initializer;
  }

}

shared_ptr<MemOnlyStorage> TestSystem::GetStorage() {
  return storage_;
}

void TestSystem::AddTxnGenerator(std::unique_ptr<Workload>&& workload, uint32_t num_txns, int num_clients,
                               int duration, std::shared_ptr<RateLimiter> rate_limiter) {
  txn_generator_.push_back(MakeRunnerFor<SynchronousTxnGenerator>(config_, *broker_->context(), std::move(workload),
                                                                  config_->local_region(), config_->local_replica(),
                                                                  num_txns, num_clients, duration, rate_limiter, false));
}

bool TestSystem::TxnGeneratorRunning(){

  CHECK(!txn_generator_.empty()) << "Test does not have a txn generator";

  bool running = false;
  for (auto & txn_gen : txn_generator_){
    running |= txn_gen->is_running();
  }
  return running;
}

std::map<int, std::array<size_t, 4>> TestSystem::TxnGeneratorProgress(){
  std::map<int, std::array<size_t, 4>> gen_statistics;

  CHECK(!txn_generator_.empty());

  for (auto & txn_generator : txn_generator_){
    auto gen = dynamic_cast<const TxnGenerator*>(txn_generator->module().get());
    gen_statistics[gen->genId()] = {gen->sent_txns(), gen->committed_txns(), gen->aborted_txns(), gen->restarted_txns()};
  }
  return gen_statistics;
}

void TestSystem::AddOutputSocket(Channel channel, const std::vector<uint64_t>& tags) {
  switch (channel) {
    case kForwarderChannel: {
      zmq::socket_t outproc_socket(*broker_->context(), ZMQ_PULL);
      outproc_socket.bind(
          MakeRemoteAddress(config_->protocol(), config_->local_address(), config_->forwarder_port(), true));
      outproc_sockets_[channel] = std::move(outproc_socket);
      break;
    }
    case kSequencerChannel: {
      zmq::socket_t outproc_socket(*broker_->context(), ZMQ_PULL);
      auto address = MakeRemoteAddress(config_->protocol(), config_->local_address(), config_->sequencer_port(), true);
      outproc_socket.bind(address);
      outproc_sockets_[channel] = std::move(outproc_socket);
      break;
    }
    default:
      broker_->AddChannel(Broker::ChannelOption(channel, false, tags));
  }

  zmq::socket_t inproc_socket(*broker_->context(), ZMQ_PULL);
  inproc_socket.bind(MakeInProcChannelAddress(channel));
  inproc_sockets_.insert_or_assign(channel, std::move(inproc_socket));
}

void TestSystem::AddSchedulerWorkerSocket() {
  auto num_workers = broker_->config()->num_workers();
  for (auto i = 0; i < num_workers; i++) {
    scheduler_worker_socket_.emplace_back(*broker_->context(), ZMQ_PAIR);
    scheduler_worker_socket_[i].bind(kSchedWorkerAddress + std::to_string(i));
  }
}

void TestSystem::AddWorkerSocketOutput() {
  auto num_workers = broker_->config()->num_workers();

  for (auto i = 0; i < num_workers; i++) {
    worker_sockets_.emplace_back(*broker_->context(), ZMQ_PAIR);
    worker_sockets_[i].connect(kSchedWorkerAddress + std::to_string(i));
  }
}

void TestSystem::SendMessageThroughWorkerSocket(int worker_id, zmq::message_t msg) {
  CHECK(worker_sockets_.size() >= worker_id+1) << "Worker " << worker_id << " does not exist";
  worker_sockets_[worker_id].send(msg, zmq::send_flags::none);
}

void TestSystem::SendMessageThroughSchedulerWorkerSocket(int worker_id, zmq::message_t msg ) {
  CHECK(scheduler_worker_socket_.size() >= worker_id+1) << "Worker " << worker_id << " does not exist";
  scheduler_worker_socket_[worker_id].send(msg, zmq::send_flags::none);
}

zmq::pollitem_t TestSystem::GetPollItemForOutputSocket(Channel channel, bool inproc) {
  if (inproc) {
    auto it = inproc_sockets_.find(channel);
    CHECK(it != inproc_sockets_.end()) << "Inproc socket " << channel << " does not exist";
    return {static_cast<void*>(it->second), 0, /* fd */
            ZMQ_POLLIN, 0 /* revent */};
  }
  auto it = outproc_sockets_.find(channel);
  CHECK(it != outproc_sockets_.end()) << "Outproc socket " << channel << " does not exist";
  return {static_cast<void*>(it->second), 0, /* fd */
          ZMQ_POLLIN, 0 /* revent */};
}

EnvelopePtr TestSystem::ReceiveFromOutputSocket(Channel channel, bool inproc) {
  if (inproc) {
    CHECK(inproc_sockets_.count(channel) > 0) << "Inproc socket \"" << channel << "\" does not exist";
    return RecvEnvelope(inproc_sockets_[channel]);
  }
  CHECK(outproc_sockets_.count(channel) > 0) << "Outproc socket \"" << channel << "\" does not exist";
  zmq::message_t msg;
  outproc_sockets_[channel].recv(msg);
  return DeserializeEnvelope(msg);
}

EnvelopePtr TestSystem::ReceiveFromOutputSocketTimeout(Channel channel, bool inproc, milliseconds wait) {
  auto now =std::chrono::high_resolution_clock::now();
  while (std::chrono::duration_cast<milliseconds >(std::chrono::high_resolution_clock::now()-now) < wait) {
    if (inproc) {
      CHECK(inproc_sockets_.count(channel) > 0) << "Inproc socket \"" << channel << "\" does not exist";
      auto ptr = RecvEnvelope(inproc_sockets_[channel], true);
      if (ptr != nullptr) { return ptr; }
      continue;
    }
    CHECK(outproc_sockets_.count(channel) > 0) << "Outproc socket \"" << channel << "\" does not exist";
    if (zmq::message_t msg; outproc_sockets_[channel].recv(msg, zmq::recv_flags::dontwait)) {
      return DeserializeEnvelope(msg);
    }
  }
  return nullptr;
}

zmq::message_t TestSystem::ReceiveFromWorkerOutputSocket(int worker_id, milliseconds wait) {

  CHECK(worker_sockets_.size() >= worker_id+1) << "Worker " << worker_id << " does not exist";

  zmq::message_t msg;

  if (wait.count() == 0) {
    worker_sockets_[worker_id].recv(msg);
  } else {
    auto now =std::chrono::high_resolution_clock::now();
    while (std::chrono::duration_cast<milliseconds >(std::chrono::high_resolution_clock::now()-now) < wait) {
      if (worker_sockets_[worker_id].recv(msg, zmq::recv_flags::dontwait)) {
        break;
      }
    }
  }
  return msg;
}

zmq::message_t TestSystem::ReceiveFromSchedulerWorkerSocket(int worker_id, milliseconds wait) {
  CHECK(scheduler_worker_socket_.size() >= worker_id+1) << "Worker " << worker_id << " does not exist";

  zmq::message_t msg;

  if (wait.count() == 0) {
    scheduler_worker_socket_[worker_id].recv(msg);
  } else {
    auto now =std::chrono::high_resolution_clock::now();
    while (std::chrono::duration_cast<milliseconds >(std::chrono::high_resolution_clock::now()-now) < wait) {
      if (scheduler_worker_socket_[worker_id].recv(msg, zmq::recv_flags::dontwait)) {
        break;
      }
    }
  }
  return msg;
}

unique_ptr<Sender> TestSystem::NewSender() { return std::make_unique<Sender>(broker_->config(), broker_->context()); }

void TestSystem::StartInNewThreads() {
  broker_->StartInNewThreads();
}

void TestSystem::StartTxnGeneratorInNewThreads() {
  if (!txn_generator_.empty()) {
    for (auto& txn_gen : txn_generator_){
      txn_gen->StartInNewThread();
    }
  }
}

void TestSystem::InitializeTPCC() {
  auto tpcc_partitioning = config_->proto_config().tpcc_partitioning();

  std::shared_ptr<MetadataInitializer> metadata_initializer;

  if (!metadata_initializer) {
    if (config_->proto_config().partitioning_case() == internal::Configuration::kTpccPartitioning){
      metadata_initializer = std::make_shared<tpcc::TPCCMetadataInitializer>(config_->num_regions(), config_->num_partitions(), config()->local_region());
    } else {
      metadata_initializer = std::make_shared<SimpleMetadataInitializer>(config_->num_regions(), config_->num_partitions());
    }
  }
  auto storage_adapter = std::make_shared<tpcc::KVStorageAdapter>(storage_, metadata_initializer);
  tpcc::LoadTables(storage_adapter, tpcc_partitioning.warehouses(), config_->num_regions(), config_->num_partitions(),
                   config_->local_partition(), config_->local_region(), 4);
}

Transaction* TestSystem::MakeTestTransaction(const ConfigurationPtr& config, TxnId id, const std::vector<KeyMetadata>& keys,
                                 const std::vector<std::vector<std::string>> code, std::optional<int> remaster,
                                 MachineId coordinator) {
  auto txn = MakeTransaction(keys, code, remaster, coordinator);
  txn->mutable_internal()->set_id(id);

  PopulateInvolvedPartitions(Sharder::MakeSharder(config), *txn);

  return txn;
}

void TestSystem::Data(Key&& key, Record&& record) {
  CHECK(sharder_->is_local_key(key)) << "Key \"" << key << "\" belongs to partition "
                                     << sharder_->compute_partition(key);

  storage_->Write(key, record);
}

}  // namespace slog