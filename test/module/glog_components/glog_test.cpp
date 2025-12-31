#include <gtest/gtest.h>

#include <vector>

#include "common/proto_utils.h"
#include "test/glog_test_utils.h"

using namespace std;
using namespace slog;
using namespace glog;
using internal::Envelope;

/**
 *  MultiSlotId Log Test
 * */

/**
 * Test Cases:
 *  - Epoch -> Advance
 *  - Advance -> Epoch
 *  - Client Removal
 * */

Batch* MakeBatch(BatchId batch_id, const vector<Transaction*>& txns, TransactionType batch_type) {
  auto* batch = new Batch();
  batch->set_id(batch_id);
  batch->set_transaction_type(batch_type);
  for (auto txn : txns) {
    batch->mutable_transactions()->AddAllocated(txn);
  }
  return batch;
}
const int NUM_REGIONS = 2;
const int NUM_REPLICAS = 2;

class GlogTest : public ::testing::Test {
 public:
  void SetUp() {
    FLAGS_v = 4;
    internal::Configuration extra_config;
    extra_config.set_log_type(slog::internal::MULTI_SLOT);
    extra_config.set_epoch_duration(milliseconds(2000).count());
    extra_config.set_start_slots(1);
    extra_config.set_min_slots(1);
    extra_config.set_rounding_factor(1);

    configs_ = MakeGlogTestConfigurations("glog_manager", NUM_REGIONS, NUM_REPLICAS, 1, extra_config);

    int counter = 0;
    // Only initialize a glog for the first region, the others just have mocks
    for (int reg = 0; reg < NUM_REGIONS; reg++) {
      for (int rep = 0; rep < NUM_REPLICAS; rep++) {
        MachineId id = MakeMachineId(reg, rep, 0);
        auto res = glogs_.emplace(id, configs_[counter]);
        CHECK(res.second);
        auto& glog = res.first->second;
        if (counter++ == 0) {
          order_sender_ = glog.NewSender();
        }
        glog.AddGlog();
        glog.AddOutputSocket(kSequencerChannel);
        senders_.emplace(id, glog.NewSender());
        glog.StartGlogInNewThreads();
      }
    }
  }

  void SendToGlog(MachineId to, MachineId from, const Envelope& req) {
    auto it = senders_.find(from);
    CHECK(it != senders_.end());
    it->second->Send(req, to, kGLOGChannel);
  }

  slog::internal::ForwardZiplogBatch* ReceiveForwardedBatch(MachineId id) {
    auto it = glogs_.find(id);
    CHECK(it != glogs_.end());

    auto req_env = it->second.ReceiveFromOutputSocket(kGLOGChannel, false);
    if (req_env == nullptr) {
      return nullptr;
    }
    if (req_env->request().type_case() != internal::Request::kZipForwardBatch) {
      return nullptr;
    }
    return req_env->mutable_request()->release_zip_forward_batch();
  }

  slog::internal::GlogBatchRequest* ReceiveEmittedBatch(MachineId id, milliseconds timeout = milliseconds(0)) {
    auto it = glogs_.find(id);
    CHECK(it != glogs_.end());

    EnvelopePtr req_env;
    if (timeout.count() == 0) {
      req_env = it->second.ReceiveFromOutputSocket(kSequencerChannel);
    } else {
      req_env = it->second.ReceiveFromOutputSocketTimeout(kSequencerChannel, true, timeout);
    }

    if (req_env == nullptr) {
      return nullptr;
    }
    if (req_env->request().type_case() != internal::Request::kGlogBatch) {
      return nullptr;
    }
    return req_env->mutable_request()->release_glog_batch();
  }

  void IssueEpoch(MachineId target, const Envelope& env) const { order_sender_->Send(env, target, kGLOGChannel); }

  template <typename Func, typename... Args>
  void Broadcast(int reps, const Func& func, Args... args) {
    for (int reg = 0; reg < NUM_REGIONS; reg++) {
      for (int rep = 0; rep < reps; rep++) {
        func(MakeMachineId(reg, rep, 0), args...);
      }
    }
  }

  struct ClientSlot {
    ClientId client_id;
    uint64_t num_slots;
    RegionId region;
  };

  EnvelopePtr MakeEpoch(uint64_t gsn_base, const std::vector<ClientSlot>& slots) {
    // Make new Epoch
    auto env = make_unique<Envelope>();

    auto epoch = env->mutable_request()->mutable_order_epoch();
    epoch->set_epoch_start(std::chrono::high_resolution_clock::now().time_since_epoch().count());
    auto regionSlots = epoch->add_regions();

    regionSlots->set_gsn_base(gsn_base);
    regionSlots->set_num_clients(slots.size());

    for (auto& slot : slots) {
      slog::internal::ZiplogClientSlot* client_slots = regionSlots->mutable_slots()->Add();
      client_slots->set_client_id(slot.client_id);
      client_slots->set_num_slots(slot.num_slots);
      client_slots->set_client_region(slot.region);
      client_slots->set_client_type(MULTI_HOME_OR_LOCK_ONLY);
    }

    return env;
  }

  EnvelopePtr MakeStubBatch(ClientSlot client, uint64_t position, Transaction* txn) {
    auto env = make_unique<Envelope>();
    auto batch_request = env->mutable_request()->mutable_zip_forward_batch();

    batch_request->set_slots(client.num_slots);
    batch_request->set_generator(client.client_id);
    batch_request->set_home(client.region);
    batch_request->set_generator_position(position);

    auto batch = batch_request->mutable_batch_data()->Add();
    batch->mutable_transactions()->Add()->CopyFrom(*txn);
    batch->set_home(client.region);

    return env;
  }

  MachineId glog_machine_id_;
  unique_ptr<Sender> order_sender_;
  unordered_map<MachineId, unique_ptr<Sender>> senders_;
  unordered_map<MachineId, TestGlog> glogs_;
  ConfigVec configs_;
};

TEST_F(GlogTest, BatchFollowsEpoch) {
  ClientSlot client{0, 2, 0};

  std::vector<ClientSlot> slots = {client};
  auto send_txn_1 = MakeTestTransaction(configs_[0], 1000, {{"A", KeyType::READ, 0}, {"B", KeyType::READ, 1}});


  for (int epoch_gsn = 0; epoch_gsn < 2; epoch_gsn++) {
    auto epoch = *MakeEpoch(epoch_gsn, slots);
    Broadcast(NUM_REPLICAS, [this, epoch](MachineId machine_id) { this->IssueEpoch(machine_id, epoch); });
    sleep(2);
  }

  // Broadcast to all glogs
  auto stub_batch = *MakeStubBatch(client, 0, send_txn_1);
  auto from = 0;
  Broadcast(NUM_REPLICAS, [this, from, stub_batch](MachineId machineId) {
    if (GET_REGION_ID(machineId) == GET_REGION_ID(from) || GET_REPLICA_ID(machineId) == GET_REGION_ID(from)) {
      this->SendToGlog(machineId, from, stub_batch);
    }
  });

  // Check the responses
  auto sent_txn = *send_txn_1;
  Broadcast(1, [this, sent_txn](MachineId machine_id) {
    auto received_batch = ReceiveEmittedBatch(machine_id, milliseconds(1000));


    if (GET_REGION_ID(machine_id) == 2) {
      ASSERT_EQ(received_batch, nullptr);
    } else {
      ASSERT_NE(received_batch, nullptr);
      ASSERT_EQ(0, received_batch->slot());
      ASSERT_TRUE(received_batch->has_txns());
      ASSERT_EQ(1, received_batch->txns().transactions_size());

      auto txn = received_batch->mutable_txns()->transactions(0);
      ASSERT_EQ(1000, txn.internal().id());
      ASSERT_EQ(2, txn.keys_size());
      ASSERT_EQ(TxnValueEntry(sent_txn, "A"), TxnValueEntry(txn, "A"));
      ASSERT_EQ(TxnValueEntry(sent_txn, "B"), TxnValueEntry(txn, "B"));
    }
  });
}

TEST_F(GlogTest, BatchRightOrder) {
  ClientSlot client_1{0, 1, 0};
  ClientSlot client_2{1, 1, 0};

  std::vector<ClientSlot> slots = {client_1, client_2};
  auto send_txn_1 = MakeTestTransaction(configs_[0], 1000,
                                        {{"A", KeyType::READ, 0}, {"B", KeyType::READ, 1}, {"C", KeyType::READ, 2}});

  auto send_txn_2 = MakeTestTransaction(configs_[0], 1001,
                                        {{"D", KeyType::READ, 0}, {"E", KeyType::READ, 1}, {"F", KeyType::READ, 2}});

  LOG(INFO) << "Epoch";

  for (int epoch_gsn = 0; epoch_gsn < 2; epoch_gsn++) {
    auto epoch = *MakeEpoch(epoch_gsn, slots);
    Broadcast(NUM_REPLICAS, [this, epoch](MachineId machine_id) { this->IssueEpoch(machine_id, epoch); });
    sleep(2);
  }

  // Broadcast to all glogs
  LOG(INFO) << "BROADCAST TO GLOG";

  {
    auto stub_batch = *MakeStubBatch(client_1, 0, send_txn_1);
    auto from = 0;
    Broadcast(NUM_REPLICAS, [this, from, stub_batch](MachineId machineId) {
      if (GET_REGION_ID(machineId) == GET_REGION_ID(from) || GET_REPLICA_ID(machineId) == GET_REGION_ID(from)) {
        this->SendToGlog(machineId, from, stub_batch);
      }
    });

    // Check that no one got a response
    Broadcast(1, [this](MachineId machine_id) {
      auto received_batch = ReceiveEmittedBatch(machine_id, milliseconds(1000));
      ASSERT_EQ(received_batch, nullptr);
    });
  }
  LOG(INFO) << "NO RESPONSE, BROADCASTING NEXT";

  // After Broadcasting the next message, everyone should receive both in order
  {
    auto stub_batch = *MakeStubBatch(client_2, 0, send_txn_2);
    MachineId from = 0;
    Broadcast(NUM_REPLICAS, [this, from, stub_batch](MachineId machineId) {
      if (GET_REGION_ID(machineId) == GET_REGION_ID(from) || GET_REPLICA_ID(machineId) == GET_REGION_ID(from)) {
        this->SendToGlog(machineId, from, stub_batch);
      }
    });

    auto sent_txn_2 = *send_txn_2;
    Broadcast(1, [this, sent_txn_2](MachineId machine_id) {
      auto received_batch = ReceiveEmittedBatch(machine_id, milliseconds(3000));

      ASSERT_NE(received_batch, nullptr);
      ASSERT_EQ(0, received_batch->slot());
      ASSERT_TRUE(received_batch->has_txns());
      ASSERT_EQ(1, received_batch->txns().transactions_size());

      auto txn = received_batch->mutable_txns()->transactions(0);
      ASSERT_EQ(1001, txn.internal().id());
      ASSERT_EQ(3, txn.keys_size());
      ASSERT_EQ(TxnValueEntry(sent_txn_2, "D"), TxnValueEntry(txn, "D"));
      ASSERT_EQ(TxnValueEntry(sent_txn_2, "E"), TxnValueEntry(txn, "E"));
      ASSERT_EQ(TxnValueEntry(sent_txn_2, "F"), TxnValueEntry(txn, "F"));
    });

    auto sent_txn_1 = *send_txn_1;
    Broadcast(1, [this, sent_txn_1](MachineId machine_id) {
      auto received_batch = ReceiveEmittedBatch(machine_id, milliseconds(1000));

      ASSERT_EQ(1, received_batch->slot());
      ASSERT_TRUE(received_batch->has_txns());
      ASSERT_EQ(1, received_batch->txns().transactions_size());

      auto txn = received_batch->mutable_txns()->transactions(0);
      ASSERT_EQ(1000, txn.internal().id());
      ASSERT_EQ(3, txn.keys_size());
      ASSERT_EQ(TxnValueEntry(sent_txn_1, "A"), TxnValueEntry(txn, "A"));
      ASSERT_EQ(TxnValueEntry(sent_txn_1, "B"), TxnValueEntry(txn, "B"));
      ASSERT_EQ(TxnValueEntry(sent_txn_1, "C"), TxnValueEntry(txn, "C"));
    });
  }
}

TEST_F(GlogTest, BatchPropagation) {
  ClientSlot client{0, 1, 0};

  std::vector<ClientSlot> slots = {client};
  auto send_txn_1 = MakeTestTransaction(configs_[0], 1000,
                                        {{"A", KeyType::READ, 0}, {"B", KeyType::READ, 1}, {"C", KeyType::WRITE, 2}});

  // Issue epoch
  for (int epoch_gsn = 0; epoch_gsn < 2; epoch_gsn++) {
    auto epoch = *MakeEpoch(epoch_gsn, slots);
    Broadcast(NUM_REPLICAS, [this, epoch](MachineId machine_id) { this->IssueEpoch(machine_id, epoch); });
    sleep(2);
  }

  // Issue Batch
  auto stub_batch = *MakeStubBatch(client, 0, send_txn_1);
  MachineId from = MakeMachineId(0, 0, 0);
  Broadcast(NUM_REPLICAS, [this, from, stub_batch](MachineId machineId) {
    if (GET_REGION_ID(machineId) == GET_REGION_ID(from) || GET_REPLICA_ID(machineId) == GET_REPLICA_ID(from)) {
      this->SendToGlog(machineId, from, stub_batch);
    }
  });

  // Sequencer in replica 0 must get all requests twice
  Broadcast(1, [this, send_txn_1](MachineId machine_id) {
    auto received_batch = ReceiveEmittedBatch(machine_id, milliseconds(1000));
    ASSERT_NE(received_batch, nullptr);
    ASSERT_EQ(0, received_batch->slot());
    ASSERT_TRUE(received_batch->has_txns());
    ASSERT_EQ(1, received_batch->txns().transactions_size());

    auto txn = received_batch->mutable_txns()->transactions(0);
    ASSERT_EQ(1000, txn.internal().id());
    ASSERT_EQ(3, txn.keys_size());
    ASSERT_EQ(TxnValueEntry(*send_txn_1, "A"), TxnValueEntry(txn, "A"));
    ASSERT_EQ(TxnValueEntry(*send_txn_1, "B"), TxnValueEntry(txn, "B"));
    ASSERT_EQ(TxnValueEntry(*send_txn_1, "C"), TxnValueEntry(txn, "C"));
  });
}