//
// Created by jrsoares on 09/10/25.
//
#pragma once

#include "common/configuration.h"
#include "common/rate_limiter.h"
#include "connection/broker.h"
#include "connection/sender.h"
#include "connection/zmq_utils.h"
#include "storage/mem_only_storage.h"
#include "storage/metadata_initializer.h"
#include "workload/workload.h"
#include "common/sharder.h"
#include "common/proto_utils.h"

using std::shared_ptr;
using std::chrono::milliseconds;
using std::unique_ptr;

namespace slog {

const auto kTestModuleTimeout = std::chrono::milliseconds(5);
using ModuleRunnerPtr = unique_ptr<ModuleRunner>;

ValueEntry TxnValueEntry(const Transaction& txn, const std::string& key);

class TestSystem {
 public:
  TestSystem(const ConfigurationPtr& config, bool init = false);
  shared_ptr<MemOnlyStorage> GetStorage();

  void AddTxnGenerator(std::unique_ptr<Workload>&& workload, uint32_t num_txns, int num_clients, int duration,
                       std::shared_ptr<RateLimiter> rate_limiter);

  bool TxnGeneratorRunning();
  std::map<int, std::array<size_t, 4>> TxnGeneratorProgress();
  void AddOutputSocket(Channel channel, const std::vector<uint64_t>& tags = {});
  void AddWorkerSocketOutput();
  void AddSchedulerWorkerSocket();

  zmq::pollitem_t GetPollItemForOutputSocket(Channel channel, bool inproc = true);
  EnvelopePtr ReceiveFromOutputSocket(Channel channel, bool inproc = true);
  EnvelopePtr ReceiveFromOutputSocketTimeout(Channel channel, bool inproc = true,
                                             milliseconds wait = milliseconds(100));
  zmq::message_t ReceiveFromWorkerOutputSocket(int worker_id, milliseconds wait = milliseconds(0));
  zmq::message_t ReceiveFromSchedulerWorkerSocket(int worker_id, milliseconds wait = milliseconds(0));

  void SendMessageThroughWorkerSocket(int worker_id, zmq::message_t msg);
  void SendMessageThroughSchedulerWorkerSocket(int worker_id, zmq::message_t msg);

  unique_ptr<Sender> NewSender();
  void StartInNewThreads();
  void StartTxnGeneratorInNewThreads();

  const ConfigurationPtr& config() const { return config_; }
  const SharderPtr& sharder() const { return sharder_; }
  std::shared_ptr<MetadataInitializer>& metadata_initializer() { return metadata_initializer_; }
  const std::shared_ptr<Broker>& broker() const { return broker_; }
  void InitializeTPCC();
  Transaction* MakeTestTransaction(const ConfigurationPtr& config, TxnId id, const std::vector<KeyMetadata>& keys,
                                  const std::vector<std::vector<std::string>> code, std::optional<int> remaster,
                                  MachineId coordinator);
 void Data(Key&& key, Record&& record);

 protected:
  shared_ptr<MetadataInitializer> metadata_initializer_;
  shared_ptr<Broker> broker_;
  ConfigurationPtr config_;
  std::unordered_map<Channel, zmq::socket_t> inproc_sockets_;
  std::unordered_map<Channel, zmq::socket_t> outproc_sockets_;
  std::vector<zmq::socket_t> worker_sockets_;
  std::vector<zmq::socket_t> scheduler_worker_socket_;

  shared_ptr<MemOnlyStorage> storage_;
  std::vector<ModuleRunnerPtr> txn_generator_;
  SharderPtr sharder_;
};
}  // namespace slog
