#include <gtest/gtest.h>

#include <vector>

#include "common/proto_utils.h"
#include "module/log_manager.h"
#include "test/test_utils.h"

using namespace std;
using namespace slog;

using internal::Envelope;
using internal::Request;

const int NUM_REGIONS = 2;
const int NUM_REPLICAS = 3;
const int NUM_PARTITIONS = 2;

class GlogSequencerTest : public ::testing::Test {
 public:
  void SetUp() {

    internal::Configuration extra_config;

    configs_ = MakeGlogTestConfigurations("sequencer", NUM_REGIONS, NUM_REPLICAS, NUM_PARTITIONS, extra_config);
    int counter = 0;

    for (int reg = 0; reg < NUM_REGIONS; reg++) {
      for (int rep = 0; rep < NUM_REPLICAS; rep++) {
        for (int p = 0; p < NUM_PARTITIONS; p++) {
          MachineId id = MakeMachineId(reg, rep, p);
          auto res = glog_.emplace(id, configs_[counter++]);
          CHECK(res.second);
          auto& glog = res.first->second;

          glog.AddSequencer();
          // There are 2 lock managers as set in the config
          glog.AddOutputSocket(kLogManagerChannel, {LogManager::MakeLogChannel(0)});
          glog.AddOutputSocket(kLogManagerChannel + 1, {LogManager::MakeLogChannel(1)});
          senders_.emplace(id, glog.NewSender());
          glog.StartInNewThreads();
        }
      }
    }
  }

  void SendToSequencer(MachineId from, MachineId to, Envelope& req) {
    auto it = senders_.find(from);
    CHECK(it != senders_.end());

    it->second->Send(req, to, kSequencerChannel);
  }

  vector<Batch> ReceiveBatches(MachineId id, int log_id, milliseconds timeout = milliseconds(0)) {
    auto it = glog_.find(id);
    CHECK(it != glog_.end());

    EnvelopePtr req_env;
    if (timeout.count() == 0){
      req_env = it->second.ReceiveFromOutputSocket(kLogManagerChannel + log_id);

    } else {
      req_env = it->second.ReceiveFromOutputSocketTimeout(kLogManagerChannel + log_id, true, timeout);
    }

    if (req_env == nullptr) {
      return {};
    }
    if (req_env->request().type_case() != Request::kForwardBatchData) {
      return {};
    }
    auto forward_batch = req_env->mutable_request()->mutable_forward_batch_data();
    vector<Batch> batches;

    for (int i = 0; i < forward_batch->batch_data_size(); i++) {
      batches.push_back(forward_batch->batch_data(i));
    }
    while (!forward_batch->batch_data().empty()) {
      forward_batch->mutable_batch_data()->ReleaseLast();
    }
    return batches;
  }

  std::unordered_map<MachineId, unique_ptr<Sender>> senders_;
  std::unordered_map<MachineId, TestSlog> glog_;

  ConfigVec configs_;
};

// Only send response after getting enough responses from f+1
TEST_F(GlogSequencerTest, CompleteReplication) {
  auto env = make_unique<Envelope>();
  auto glog_batch = env->mutable_request()->mutable_glog_batch();

  glog_batch->set_slot(0);
  sleep(2);
  for (ReplicaId rep = 0; rep < NUM_REPLICAS-1; rep++){
    SendToSequencer(MakeMachineId(0, rep, 0), MakeMachineId(0,0,0), *env);
    // Check that it did not send the batch
    auto batches = ReceiveBatches(0,0, milliseconds(1000));
    ASSERT_TRUE(batches.empty());
  }

  // Now send with the batch content, it should now return
  auto txn = MakeTestTransaction(configs_[0], 1000,
                                 {{"A", KeyType::READ, 0}, {"B", KeyType::READ, 0}, {"C", KeyType::WRITE, 0}});

  auto batch = glog_batch->mutable_txns();
  batch->add_transactions()->CopyFrom(*txn);
  SendToSequencer(MakeMachineId(0, NUM_REPLICAS-1, 0), MakeMachineId(0,0,0), *env);

  // Batch partitions are distributed to corresponding local partitions
  {
    auto batches = ReceiveBatches(MakeMachineId(0,0,0), 0);
    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].transactions_size(), 1);
    ASSERT_EQ(batches[0].transaction_type(), TransactionType::SINGLE_HOME);
    auto& batched_txn = batches[0].transactions().at(0);
    ASSERT_EQ(batched_txn.internal().id(), 1000);
    ASSERT_EQ(batched_txn.keys_size(), 2);
    ASSERT_EQ(TxnValueEntry(batched_txn, "A"), TxnValueEntry(*txn, "A"));
    ASSERT_EQ(TxnValueEntry(batched_txn, "C"), TxnValueEntry(*txn, "C"));
  }

  {
    auto batches = ReceiveBatches(MakeMachineId(0,0,1), 0);
    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].transactions_size(), 1);
    ASSERT_EQ(batches[0].transaction_type(), TransactionType::SINGLE_HOME);
    auto& batched_txn = batches[0].transactions().at(0);
    ASSERT_EQ(batched_txn.internal().id(), 1000);
    ASSERT_EQ(batched_txn.keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(batched_txn, "B"), TxnValueEntry(*txn, "B"));
  }

  // All batch partitions are sent to a machine in the remote replica
  {
    auto batches = ReceiveBatches(MakeMachineId(1,0,1), 0);
    ASSERT_EQ(batches.size(), 2);
    ASSERT_EQ(batches[0].transactions_size(), 1);
    ASSERT_EQ(batches[0].transaction_type(), TransactionType::SINGLE_HOME);
    {
      auto& batched_txn = batches[0].transactions().at(0);
      ASSERT_EQ(batched_txn.internal().id(), 1000);
      ASSERT_EQ(batched_txn.keys_size(), 2);
      ASSERT_EQ(TxnValueEntry(batched_txn, "A"), TxnValueEntry(*txn, "A"));
      ASSERT_EQ(TxnValueEntry(batched_txn, "C"), TxnValueEntry(*txn, "C"));
    }
    ASSERT_EQ(batches[1].transactions_size(), 1);
    ASSERT_EQ(batches[1].transaction_type(), TransactionType::SINGLE_HOME);
    {
      auto& batched_txn = batches[1].transactions().at(0);
      ASSERT_EQ(batched_txn.internal().id(), 1000);
      ASSERT_EQ(batched_txn.keys_size(), 1);
      ASSERT_EQ(TxnValueEntry(batched_txn, "B"), TxnValueEntry(*txn, "B"));
    }
  }
}

TEST_F(GlogSequencerTest, BatchThenAck) {
  auto env = make_unique<Envelope>();
  auto glog_batch = env->mutable_request()->mutable_glog_batch();

  glog_batch->set_slot(0);

  // Now send with the batch content, it should now return
  auto txn = MakeTestTransaction(configs_[0], 1000,
                                 {{"A", KeyType::READ, 0}, {"B", KeyType::READ, 0}, {"C", KeyType::WRITE, 0}});

  auto batch = glog_batch->mutable_txns();
  batch->add_transactions()->CopyFrom(*txn);

  SendToSequencer(MakeMachineId(0, NUM_REPLICAS-1, 0), MakeMachineId(0,0,0), *env);
  {
    auto batches = ReceiveBatches(0, 0, milliseconds(1000));
    ASSERT_TRUE(batches.empty());
  }
  // Now send an ack
  glog_batch->clear_txns();
  SendToSequencer(MakeMachineId(0, 0, 0), MakeMachineId(0,0,0), *env);

  // Batch partitions are distributed to corresponding local partitions
  {
    auto batches = ReceiveBatches(MakeMachineId(0,0,0), 0);
    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].transactions_size(), 1);
    ASSERT_EQ(batches[0].transaction_type(), TransactionType::SINGLE_HOME);
    auto& batched_txn = batches[0].transactions().at(0);
    ASSERT_EQ(batched_txn.internal().id(), 1000);
    ASSERT_EQ(batched_txn.keys_size(), 2);
    ASSERT_EQ(TxnValueEntry(batched_txn, "A"), TxnValueEntry(*txn, "A"));
    ASSERT_EQ(TxnValueEntry(batched_txn, "C"), TxnValueEntry(*txn, "C"));
  }

  {
    auto batches = ReceiveBatches(MakeMachineId(0,0,1), 0);
    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].transactions_size(), 1);
    ASSERT_EQ(batches[0].transaction_type(), TransactionType::SINGLE_HOME);
    auto& batched_txn = batches[0].transactions().at(0);
    ASSERT_EQ(batched_txn.internal().id(), 1000);
    ASSERT_EQ(batched_txn.keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(batched_txn, "B"), TxnValueEntry(*txn, "B"));
  }

  // All batch partitions are sent to a machine in the remote replica
  {
    auto batches = ReceiveBatches(MakeMachineId(1,0,1), 0);
    ASSERT_EQ(batches.size(), 2);
    ASSERT_EQ(batches[0].transactions_size(), 1);
    ASSERT_EQ(batches[0].transaction_type(), TransactionType::SINGLE_HOME);
    {
      auto& batched_txn = batches[0].transactions().at(0);
      ASSERT_EQ(batched_txn.internal().id(), 1000);
      ASSERT_EQ(batched_txn.keys_size(), 2);
      ASSERT_EQ(TxnValueEntry(batched_txn, "A"), TxnValueEntry(*txn, "A"));
      ASSERT_EQ(TxnValueEntry(batched_txn, "C"), TxnValueEntry(*txn, "C"));
    }
    ASSERT_EQ(batches[1].transactions_size(), 1);
    ASSERT_EQ(batches[1].transaction_type(), TransactionType::SINGLE_HOME);
    {
      auto& batched_txn = batches[1].transactions().at(0);
      ASSERT_EQ(batched_txn.internal().id(), 1000);
      ASSERT_EQ(batched_txn.keys_size(), 1);
      ASSERT_EQ(TxnValueEntry(batched_txn, "B"), TxnValueEntry(*txn, "B"));
    }
  }
}

TEST_F(GlogSequencerTest, BatchAckConcurrency) {
  auto env = make_unique<Envelope>();
  auto glog_batch = env->mutable_request()->mutable_glog_batch();

  glog_batch->set_slot(0);

  // Now send with the batch content, it should now return
  auto txn = MakeTestTransaction(configs_[0], 1000,
                                 {{"A", KeyType::READ, 0}, {"B", KeyType::READ, 0}, {"C", KeyType::WRITE, 0}});
  {

  auto batch = glog_batch->mutable_txns();
  batch->add_transactions()->CopyFrom(*txn);
  }

  SendToSequencer(MakeMachineId(0, NUM_REPLICAS-1, 0), MakeMachineId(0,0,0), *env);

  {
    auto batches = ReceiveBatches(0, 0, milliseconds(1000));
    ASSERT_TRUE(batches.empty());
  }

  auto env_2 = make_unique<Envelope>();
  auto glog_batch_2 = env_2->mutable_request()->mutable_glog_batch();

  glog_batch_2->set_slot(1);

  auto txn_2 = MakeTestTransaction(configs_[0], 1001,
                                 {{"D", KeyType::READ, 0}, {"E", KeyType::READ, 0}, {"F", KeyType::WRITE, 0}});

  {
    auto batch = glog_batch_2->mutable_txns();
    batch->add_transactions()->CopyFrom(*txn_2);
  }
  SendToSequencer(MakeMachineId(0, NUM_REPLICAS-1, 0), MakeMachineId(0,0,0), *env_2);
  {
    auto batches = ReceiveBatches(0, 0, milliseconds(1000));
    ASSERT_TRUE(batches.empty());
  }

  // Get acks for second batch, but still no response
  glog_batch_2->clear_txns();
  SendToSequencer(MakeMachineId(0, 0, 0), MakeMachineId(0,0,0), *env_2);
  {
    auto batches = ReceiveBatches(0, 0, milliseconds(1000));
    ASSERT_TRUE(batches.empty());
  }

  // Finally, send final ack of batch 1 and get both batches in order
  glog_batch->clear_txns();

  SendToSequencer(MakeMachineId(0, 0, 0), MakeMachineId(0,0,0), *env);

  // Batch partitions are distributed to corresponding local partitions
  {
    {
      auto batches = ReceiveBatches(MakeMachineId(0,0,0), 0);
      ASSERT_EQ(batches.size(), 1);
      ASSERT_EQ(batches[0].transactions_size(), 2);
      ASSERT_EQ(batches[0].transaction_type(), TransactionType::SINGLE_HOME);
      {
        auto& batched_txn = batches[0].transactions().at(0);
        ASSERT_EQ(batched_txn.internal().id(), 1000);
        ASSERT_EQ(batched_txn.keys_size(), 2);
        ASSERT_EQ(TxnValueEntry(batched_txn, "A"), TxnValueEntry(*txn, "A"));
        ASSERT_EQ(TxnValueEntry(batched_txn, "C"), TxnValueEntry(*txn, "C"));
      }
      {
        auto& batched_txn = batches[0].transactions().at(1);
        ASSERT_EQ(batched_txn.internal().id(), 1001);
        ASSERT_EQ(batched_txn.keys_size(), 1);
        ASSERT_EQ(TxnValueEntry(batched_txn, "E"), TxnValueEntry(*txn_2, "E"));
      }
    }

  }

  {

    {
      auto batches = ReceiveBatches(MakeMachineId(0,0,1), 0);
      ASSERT_EQ(batches.size(), 1);
      ASSERT_EQ(batches[0].transactions_size(), 2);
      ASSERT_EQ(batches[0].transaction_type(), TransactionType::SINGLE_HOME);
      {
        auto& batched_txn = batches[0].transactions().at(0);
        ASSERT_EQ(batched_txn.internal().id(), 1000);
        ASSERT_EQ(batched_txn.keys_size(), 1);
        ASSERT_EQ(TxnValueEntry(batched_txn, "B"), TxnValueEntry(*txn, "B"));
      }
      {
        auto& batched_txn = batches[0].transactions().at(1);
        ASSERT_EQ(batched_txn.internal().id(), 1001);
        ASSERT_EQ(batched_txn.keys_size(), 2);
        ASSERT_EQ(TxnValueEntry(batched_txn, "D"), TxnValueEntry(*txn_2, "D"));
        ASSERT_EQ(TxnValueEntry(batched_txn, "F"), TxnValueEntry(*txn_2, "F"));
      }
    }
  }

  // All batch partitions are sent to a machine in the remote replica
  {
    {
      auto batches = ReceiveBatches(MakeMachineId(1,0,1), 0);
      ASSERT_EQ(batches.size(), 2);
      ASSERT_EQ(batches[0].transactions_size(), 2);
      ASSERT_EQ(batches[0].transaction_type(), TransactionType::SINGLE_HOME);
      {
        auto& batched_txn = batches[0].transactions().at(0);
        ASSERT_EQ(batched_txn.internal().id(), 1000);
        ASSERT_EQ(batched_txn.keys_size(), 2);
        ASSERT_EQ(TxnValueEntry(batched_txn, "A"), TxnValueEntry(*txn, "A"));
        ASSERT_EQ(TxnValueEntry(batched_txn, "C"), TxnValueEntry(*txn, "C"));
      }
      {
        auto& batched_txn = batches[0].transactions().at(1);
        ASSERT_EQ(batched_txn.internal().id(), 1001);
        ASSERT_EQ(batched_txn.keys_size(), 1);
        ASSERT_EQ(TxnValueEntry(batched_txn, "E"), TxnValueEntry(*txn_2, "E"));
      }

      ASSERT_EQ(batches[1].transactions_size(), 2);
      ASSERT_EQ(batches[1].transaction_type(), TransactionType::SINGLE_HOME);
      {
        auto& batched_txn = batches[1].transactions().at(0);
        ASSERT_EQ(batched_txn.internal().id(), 1000);
        ASSERT_EQ(batched_txn.keys_size(), 1);
        ASSERT_EQ(TxnValueEntry(batched_txn, "B"), TxnValueEntry(*txn, "B"));
      }
      {
        auto& batched_txn = batches[1].transactions().at(1);
        ASSERT_EQ(batched_txn.internal().id(), 1001);
        ASSERT_EQ(batched_txn.keys_size(), 2);
        ASSERT_EQ(TxnValueEntry(batched_txn, "D"), TxnValueEntry(*txn_2, "D"));
        ASSERT_EQ(TxnValueEntry(batched_txn, "F"), TxnValueEntry(*txn_2, "F"));
      }
    }
  }
}


