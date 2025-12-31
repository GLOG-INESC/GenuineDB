//
// Created by jrsoares on 26-06-2025.
//

#include <gtest/gtest.h>

#include <vector>

#include "common/proto_utils.h"
#include "test/test_utils.h"
#include "module/log_manager.h"

using namespace std;
using namespace slog;

using internal::Envelope;
using internal::Request;

const int kNumRegions = 2;
const int kNumReplicas = 1;
const int kNumPartitions = 2;

class ForwarderSequencerTest : public ::testing::Test {

 public:
  unique_ptr<TestSlog> test_slogs_[kNumRegions*kNumReplicas*kNumPartitions];
  ConfigVec configs_;

 protected:
  virtual internal::Configuration CustomConfig() { return internal::Configuration(); }

  vector<Batch> ReceiveBatches(int i, int log_id) {
    auto req_env = test_slogs_[i]->ReceiveFromOutputSocket(kLogManagerChannel + log_id);
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
  void SetUp() {

    auto custom_config = CustomConfig();

    custom_config.set_forwarder_batch_duration(0);
    custom_config.set_bypass_mh_orderer(true);
    custom_config.set_fs_latency_interval(200);
    custom_config.set_avg_latency_window_size(10);
    custom_config.set_timestamp_buffer_us(0);

    custom_config.set_synchronized_batching(true);
    custom_config.set_sequencer_batch_duration(0);
    custom_config.set_sequencer_batch_size(100);
    custom_config.set_sequencer_rrr(true);

    custom_config.mutable_simple_partitioning()->set_num_records(2500000);
    custom_config.mutable_simple_partitioning()->set_record_size_bytes(100);

    configs_ =
        MakeTestConfigurations("e2e", kNumRegions, kNumReplicas, kNumPartitions, custom_config);

    int counter = 0;
    for (int reg = 0; reg < kNumRegions; reg++) {
      for (int rep = 0; rep < kNumReplicas; rep++) {
        for (int p = 0; p < kNumPartitions; p++) {
          auto config = configs_[counter];
          test_slogs_[counter] = make_unique<TestSlog>(config);
          test_slogs_[counter]->AddServerAndClient();
          test_slogs_[counter]->AddSimpleMetadataForwarder();
          test_slogs_[counter]->AddSequencer();

          test_slogs_[counter]->AddOutputSocket(kLogManagerChannel, {LogManager::MakeLogChannel(0)});
          test_slogs_[counter]->AddOutputSocket(kLogManagerChannel + 1, {LogManager::MakeLogChannel(1)});
          counter++;
        }
      }
    }

    for (const auto& test_slog : test_slogs_) {
      test_slog->StartInNewThreads();
    }
  }

};

TEST_F(ForwarderSequencerTest, ForwardToSameRegion) {

  // This txn needs to lookup from both partitions in a region
  auto send_txn = MakeTestTransaction(configs_[0], 1000,
                                 {{"0", KeyType::READ, 0}, {"1", KeyType::READ, 0}, {"4", KeyType::WRITE, 0}});

  auto txn = *send_txn;
  // Send to partition 0 of region 0
  test_slogs_[0]->SendTxn(send_txn);

  // Batch partitions are distributed to corresponding local partitions
  {
    auto batches = ReceiveBatches(0, 0);
    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].transactions_size(), 1);
    ASSERT_EQ(batches[0].transaction_type(), TransactionType::SINGLE_HOME);
    auto& batched_txn = batches[0].transactions().at(0);
    ASSERT_EQ(batched_txn.keys_size(), 2);
    ASSERT_EQ(TxnValueEntry(batched_txn, "0"), TxnValueEntry(txn, "0"));
    ASSERT_EQ(TxnValueEntry(batched_txn, "4"), TxnValueEntry(txn, "4"));
    ASSERT_EQ(batched_txn.internal().involved_partitions_size(), 2);
  }

  {
    auto batches = ReceiveBatches(1, 0);
    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].transactions_size(), 1);
    ASSERT_EQ(batches[0].transaction_type(), TransactionType::SINGLE_HOME);
    auto& batched_txn = batches[0].transactions().at(0);
    ASSERT_EQ(batched_txn.keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(batched_txn, "1"), TxnValueEntry(txn, "1"));
    ASSERT_EQ(batched_txn.internal().involved_partitions_size(), 2);
  }

  // All batch partitions are sent to a machine in the remote replica
  {
    auto batches = ReceiveBatches(2, 0);
    ASSERT_EQ(batches.size(), 2);
    ASSERT_EQ(batches[0].transactions_size(), 1);
    ASSERT_EQ(batches[0].transaction_type(), TransactionType::SINGLE_HOME);
    {
      auto& batched_txn = batches[0].transactions().at(0);
      ASSERT_EQ(batched_txn.keys_size(), 2);
      ASSERT_EQ(TxnValueEntry(batched_txn, "0"), TxnValueEntry(txn, "0"));
      ASSERT_EQ(TxnValueEntry(batched_txn, "4"), TxnValueEntry(txn, "4"));
      ASSERT_EQ(batched_txn.internal().involved_partitions_size(), 2);

    }
    ASSERT_EQ(batches[1].transactions_size(), 1);
    ASSERT_EQ(batches[1].transaction_type(), TransactionType::SINGLE_HOME);
    {
      auto& batched_txn = batches[1].transactions().at(0);
      ASSERT_EQ(batched_txn.keys_size(), 1);
      ASSERT_EQ(TxnValueEntry(batched_txn, "1"), TxnValueEntry(txn, "1"));
      ASSERT_EQ(batched_txn.internal().involved_partitions_size(), 2);

    }
  }
}

TEST_F(ForwarderSequencerTest, MultiHomeTransaction) {

  auto send_txn = MakeTestTransaction(
      configs_[0], 1000,
      {{"0", KeyType::READ, 0}, {"1", KeyType::READ, 0}, {"2", KeyType::WRITE, 1}, {"3", KeyType::WRITE, 1}});

  auto txn = *send_txn;
  // Send to partition 0 of region 0
  test_slogs_[0]->SendTxn(send_txn);

  // The txn was sent to region 0, which generates two subtxns, each for each partitions. These subtxns are
  // also replicated to region 1.
  {
    auto batches = ReceiveBatches(0, 0);
    ASSERT_EQ(batches.size(), 1);
    auto lo_txn = batches[0].transactions().at(0);
    ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(lo_txn.internal().home(), 0);
    ASSERT_EQ(lo_txn.keys_size(), 2);
    ASSERT_EQ(TxnValueEntry(lo_txn, "0"), TxnValueEntry(txn, "0"));
    ASSERT_EQ(TxnValueEntry(lo_txn, "2"), TxnValueEntry(txn, "2"));
    ASSERT_EQ(lo_txn.internal().involved_partitions_size(), 4);

  }

  {
    auto batches = ReceiveBatches(1, 0);
    ASSERT_EQ(batches.size(), 1);
    auto lo_txn = batches[0].transactions().at(0);
    ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(lo_txn.internal().home(), 0);
    ASSERT_EQ(lo_txn.keys_size(), 2);
    ASSERT_EQ(TxnValueEntry(lo_txn, "1"), TxnValueEntry(txn, "1"));
    ASSERT_EQ(TxnValueEntry(lo_txn, "3"), TxnValueEntry(txn, "3"));
    ASSERT_EQ(lo_txn.internal().involved_partitions_size(), 4);

  }

  {
    auto batches = ReceiveBatches(2, 0);
    ASSERT_FALSE(batches.empty());
    ASSERT_EQ(batches.size(), 2);
    {
      auto lo_txn = batches[0].transactions().at(0);
      ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
      ASSERT_EQ(lo_txn.internal().home(), 0);
      ASSERT_EQ(lo_txn.keys_size(), 2);
      ASSERT_EQ(TxnValueEntry(lo_txn, "0"), TxnValueEntry(txn, "0"));
      ASSERT_EQ(TxnValueEntry(lo_txn, "2"), TxnValueEntry(txn, "2"));
      ASSERT_EQ(lo_txn.internal().involved_partitions_size(), 4);

    }
    {
      auto lo_txn = batches[1].transactions().at(0);
      ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
      ASSERT_EQ(lo_txn.internal().home(), 0);
      ASSERT_EQ(lo_txn.keys_size(), 2);
      ASSERT_EQ(TxnValueEntry(lo_txn, "1"), TxnValueEntry(txn, "1"));
      ASSERT_EQ(TxnValueEntry(lo_txn, "3"), TxnValueEntry(txn, "3"));
      ASSERT_EQ(lo_txn.internal().involved_partitions_size(), 4);

    }
  }
}

TEST_F(ForwarderSequencerTest, MultiHomeTransaction2) {

  auto send_txn = MakeTestTransaction(
      configs_[0], 1000,
      {{"0", KeyType::READ, 0}, {"1", KeyType::READ, 0}, {"2", KeyType::WRITE, 1}, {"3", KeyType::WRITE, 1}});

  auto txn = *send_txn;
  // Send to partition 0 of region 0
  test_slogs_[0]->SendTxn(send_txn);

  // The txn was sent to region 0, which generates two subtxns, each for each partitions. These subtxns are
  // also replicated to region 1.
  {
    auto batches = ReceiveBatches(0, 0);
    ASSERT_EQ(batches.size(), 1);

    auto lo_txn = batches[0].transactions().at(0);
    ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(lo_txn.internal().home(), 0);
    ASSERT_EQ(lo_txn.keys_size(), 2);
    ASSERT_EQ(TxnValueEntry(lo_txn, "0"), TxnValueEntry(txn, "0"));
    ASSERT_EQ(TxnValueEntry(lo_txn, "2"), TxnValueEntry(txn, "2"));
    ASSERT_EQ(lo_txn.internal().involved_partitions_size(), 4);
  }

  {
    auto batches = ReceiveBatches(1, 0);

    ASSERT_EQ(batches.size(), 1);

    auto lo_txn = batches[0].transactions().at(0);
    ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(lo_txn.internal().home(), 0);
    ASSERT_EQ(lo_txn.keys_size(), 2);
    ASSERT_EQ(TxnValueEntry(lo_txn, "1"), TxnValueEntry(txn, "1"));
    ASSERT_EQ(TxnValueEntry(lo_txn, "3"), TxnValueEntry(txn, "3"));
    ASSERT_EQ(lo_txn.internal().involved_partitions_size(), 4);

  }

  {
    auto batches = ReceiveBatches(2, 0);
    ASSERT_FALSE(batches.empty());
    ASSERT_EQ(batches.size(), 2);

    ASSERT_EQ(batches[0].transactions_size(), 1);
    {
      auto lo_txn = batches[0].transactions().at(0);
      ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
      ASSERT_EQ(lo_txn.internal().home(), 0);
      ASSERT_EQ(lo_txn.keys_size(), 2);
      ASSERT_EQ(TxnValueEntry(lo_txn, "0"), TxnValueEntry(txn, "0"));
      ASSERT_EQ(TxnValueEntry(lo_txn, "2"), TxnValueEntry(txn, "2"));
      ASSERT_EQ(lo_txn.internal().involved_partitions_size(), 4);

    }

    {
      auto lo_txn = batches[1].transactions().at(0);
      ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
      ASSERT_EQ(lo_txn.internal().home(), 0);
      ASSERT_EQ(lo_txn.keys_size(), 2);
      ASSERT_EQ(TxnValueEntry(lo_txn, "1"), TxnValueEntry(txn, "1"));
      ASSERT_EQ(TxnValueEntry(lo_txn, "3"), TxnValueEntry(txn, "3"));
      ASSERT_EQ(lo_txn.internal().involved_partitions_size(), 4);
    }

  }
}
