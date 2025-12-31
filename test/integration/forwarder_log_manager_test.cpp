//
// Created by jrsoares on 26-06-2025.
//

#include <gtest/gtest.h>

#include <vector>

#include "common/proto_utils.h"
#include "module/log_manager.h"
#include "test/test_utils.h"

using namespace std;
using namespace slog;

using internal::Envelope;
using internal::Request;

const int kNumRegions = 2;
const int kNumReplicas = 1;
const int kNumPartitions = 2;

class ForwarderLogManagerTest : public ::testing::Test {
 public:
  unique_ptr<TestSlog> test_slogs_[kNumRegions * kNumReplicas * kNumPartitions];
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

    custom_config.set_num_log_managers(2);

    configs_ = MakeTestConfigurations("e2e", kNumRegions, kNumReplicas, kNumPartitions, custom_config);

    int counter = 0;
    for (int reg = 0; reg < kNumRegions; reg++) {
      for (int rep = 0; rep < kNumReplicas; rep++) {
        for (int p = 0; p < kNumPartitions; p++) {
          auto config = configs_[counter];
          test_slogs_[counter] = make_unique<TestSlog>(config);
          test_slogs_[counter]->AddServerAndClient();
          test_slogs_[counter]->AddSimpleMetadataForwarder();
          test_slogs_[counter]->AddSequencer();
          test_slogs_[counter]->AddLogManagers();
          test_slogs_[counter]->AddLocalPaxos();

          test_slogs_[counter]->AddOutputSocket(kSchedulerChannel);
          counter++;
        }
      }
    }

    for (const auto& test_slog : test_slogs_) {
      test_slog->StartInNewThreads();
    }
  }

  Transaction* ReceiveTxn(int i) {
    auto req_env = test_slogs_[i]->ReceiveFromOutputSocket(kSchedulerChannel);
    if (req_env == nullptr) {
      return nullptr;
    }
    if (req_env->request().type_case() != internal::Request::kForwardTxn) {
      return nullptr;
    }
    return req_env->mutable_request()->mutable_forward_txn()->release_txn();
  }
};

TEST_F(ForwarderLogManagerTest, SingleHomeTransaction) {
  // This txn needs to lookup from both partitions in a region
  auto send_txn = MakeTestTransaction(configs_[0], 1000,
                                      {{"0", KeyType::READ, 0}, {"1", KeyType::READ, 0}, {"4", KeyType::WRITE, 0}});

  auto txn = *send_txn;
  // Send to partition 0 of region 0
  test_slogs_[0]->SendTxn(send_txn);

  {
    auto sent_txn = ReceiveTxn(0);
    ASSERT_EQ(sent_txn->internal().home(), 0);
    ASSERT_EQ(sent_txn->internal().type(), TransactionType::SINGLE_HOME);
    ASSERT_EQ(sent_txn->internal().involved_partitions_size(), 2);
    ASSERT_EQ(sent_txn->internal().involved_partitions(0), 0);
    ASSERT_EQ(sent_txn->internal().involved_partitions(1), 1);

    ASSERT_EQ(sent_txn->internal().active_partitions_size(), 1);
    ASSERT_EQ(sent_txn->internal().active_partitions(0), 0);

    ASSERT_EQ(sent_txn->internal().involved_regions_size(), 1);
    ASSERT_EQ(sent_txn->internal().involved_regions(0), 0);

    ASSERT_EQ(sent_txn->keys_size(), 2);
    ASSERT_EQ(TxnValueEntry(*sent_txn, "0"), TxnValueEntry(txn, "0"));
    ASSERT_EQ(TxnValueEntry(*sent_txn, "4"), TxnValueEntry(txn, "4"));
  }

  {
    auto sent_txn = ReceiveTxn(1);
    ASSERT_EQ(sent_txn->internal().home(), 0);

    ASSERT_EQ(sent_txn->internal().type(), TransactionType::SINGLE_HOME);
    ASSERT_EQ(sent_txn->internal().involved_partitions_size(), 2);
    ASSERT_EQ(sent_txn->internal().involved_partitions(0), 0);
    ASSERT_EQ(sent_txn->internal().involved_partitions(1), 1);

    ASSERT_EQ(sent_txn->internal().active_partitions_size(), 1);
    ASSERT_EQ(sent_txn->internal().active_partitions(0), 0);

    ASSERT_EQ(sent_txn->internal().involved_regions_size(), 1);
    ASSERT_EQ(sent_txn->internal().involved_regions(0), 0);

    ASSERT_EQ(sent_txn->keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(*sent_txn, "1"), TxnValueEntry(txn, "1"));
  }

  {
    auto sent_txn = ReceiveTxn(2);
    ASSERT_EQ(sent_txn->internal().home(), 0);

    ASSERT_EQ(sent_txn->internal().type(), TransactionType::SINGLE_HOME);
    ASSERT_EQ(sent_txn->internal().involved_partitions_size(), 2);
    ASSERT_EQ(sent_txn->internal().involved_partitions(0), 0);
    ASSERT_EQ(sent_txn->internal().involved_partitions(1), 1);

    ASSERT_EQ(sent_txn->internal().active_partitions_size(), 1);
    ASSERT_EQ(sent_txn->internal().active_partitions(0), 0);

    ASSERT_EQ(sent_txn->internal().involved_regions_size(), 1);
    ASSERT_EQ(sent_txn->internal().involved_regions(0), 0);

    ASSERT_EQ(sent_txn->keys_size(), 2);
    ASSERT_EQ(TxnValueEntry(*sent_txn, "0"), TxnValueEntry(txn, "0"));
    ASSERT_EQ(TxnValueEntry(*sent_txn, "4"), TxnValueEntry(txn, "4"));
  }

  {
    auto sent_txn = ReceiveTxn(3);
    ASSERT_EQ(sent_txn->internal().home(), 0);

    ASSERT_EQ(sent_txn->internal().type(), TransactionType::SINGLE_HOME);
    ASSERT_EQ(sent_txn->internal().involved_partitions_size(), 2);
    ASSERT_EQ(sent_txn->internal().involved_partitions(0), 0);
    ASSERT_EQ(sent_txn->internal().involved_partitions(1), 1);

    ASSERT_EQ(sent_txn->internal().active_partitions_size(), 1);
    ASSERT_EQ(sent_txn->internal().active_partitions(0), 0);

    ASSERT_EQ(sent_txn->internal().involved_regions_size(), 1);
    ASSERT_EQ(sent_txn->internal().involved_regions(0), 0);

    ASSERT_EQ(sent_txn->keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(*sent_txn, "1"), TxnValueEntry(txn, "1"));
  }
}

TEST_F(ForwarderLogManagerTest, MultiHomeSinglePartitionTransaction) {
  // This txn needs to lookup from both partitions in a region
  auto send_txn = MakeTestTransaction(configs_[0], 1000, {{"0", KeyType::READ, 0}, {"2", KeyType::WRITE, 1}});

  auto txn = *send_txn;
  // Send to partition 0 of region 0
  test_slogs_[0]->SendTxn(send_txn);

  for (int i = 0; i < 2; i++) {
    std::set<int> homes = std::set<int>();
    for (int j = 0; j < 2; j++) {
      auto sent_txn = ReceiveTxn(i * 2);
      ASSERT_EQ(sent_txn->internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
      ASSERT_EQ(sent_txn->internal().involved_partitions_size(), 2);
      ASSERT_EQ(sent_txn->internal().involved_partitions(0), 0);
      ASSERT_EQ(sent_txn->internal().involved_partitions(1), MakeMachineId(1, 0, 0));

      ASSERT_EQ(sent_txn->internal().active_partitions_size(), 1);
      ASSERT_EQ(sent_txn->internal().active_partitions(0), MakeMachineId(1, 0, 0));

      ASSERT_EQ(sent_txn->internal().involved_regions_size(), 2);
      ASSERT_EQ(sent_txn->internal().involved_regions(0), 0);
      ASSERT_EQ(sent_txn->internal().involved_regions(1), 1);

      ASSERT_EQ(sent_txn->keys_size(), 2);
      ASSERT_EQ(TxnValueEntry(*sent_txn, "0"), TxnValueEntry(txn, "0"));
      ASSERT_EQ(TxnValueEntry(*sent_txn, "2"), TxnValueEntry(txn, "2"));

      ASSERT_TRUE(homes.count(sent_txn->internal().home()) == 0);
      ASSERT_TRUE(sent_txn->internal().home() == 0 || sent_txn->internal().home() == 1);
      homes.insert(sent_txn->internal().home());
    }
  }
}

TEST_F(ForwarderLogManagerTest, MultiHomeMultiPartitionTransaction) {
  // This txn needs to lookup from both partitions in a region
  auto send_txn = MakeTestTransaction(
      configs_[0], 1000,
      {{"0", KeyType::READ, 0}, {"1", KeyType::READ, 0}, {"2", KeyType::WRITE, 1}, {"3", KeyType::WRITE, 1}});

  auto txn = *send_txn;
  // Send to partition 0 of region 0
  test_slogs_[0]->SendTxn(send_txn);

  for (int i = 0; i < 2; i++) {
    std::set<int> homes = std::set<int>();
    for (int j = 0; j < 2; j++) {
      auto sent_txn = ReceiveTxn(i * 2);
      ASSERT_EQ(sent_txn->internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
      ASSERT_EQ(sent_txn->internal().involved_partitions_size(), 4);
      ASSERT_EQ(sent_txn->internal().involved_partitions(0), 0);
      ASSERT_EQ(sent_txn->internal().involved_partitions(1), 1);
      ASSERT_EQ(sent_txn->internal().involved_partitions(2), MakeMachineId(1, 0, 0));
      ASSERT_EQ(sent_txn->internal().involved_partitions(3), MakeMachineId(1, 0, 1));

      ASSERT_EQ(sent_txn->internal().active_partitions_size(), 2);
      ASSERT_EQ(sent_txn->internal().active_partitions(0), MakeMachineId(1, 0, 0));
      ASSERT_EQ(sent_txn->internal().active_partitions(1), MakeMachineId(1, 0, 1));

      ASSERT_EQ(sent_txn->internal().involved_regions_size(), 2);
      ASSERT_EQ(sent_txn->internal().involved_regions(0), 0);
      ASSERT_EQ(sent_txn->internal().involved_regions(1), 1);

      ASSERT_EQ(sent_txn->keys_size(), 2);
      ASSERT_EQ(TxnValueEntry(*sent_txn, "0"), TxnValueEntry(txn, "0"));
      ASSERT_EQ(TxnValueEntry(*sent_txn, "2"), TxnValueEntry(txn, "2"));

      ASSERT_TRUE(homes.count(sent_txn->internal().home()) == 0);
      ASSERT_TRUE(sent_txn->internal().home() == 0 || sent_txn->internal().home() == 1);
      homes.insert(sent_txn->internal().home());
    }
  }

  for (int i = 0; i < 2; i++) {
    std::set<int> homes = std::set<int>();
    for (int j = 0; j < 2; j++) {
      auto sent_txn = ReceiveTxn(1 + i * 2);
      ASSERT_EQ(sent_txn->internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
      ASSERT_EQ(sent_txn->internal().involved_partitions_size(), 4);
      ASSERT_EQ(sent_txn->internal().involved_partitions(0), 0);
      ASSERT_EQ(sent_txn->internal().involved_partitions(1), 1);
      ASSERT_EQ(sent_txn->internal().involved_partitions(2), MakeMachineId(1, 0, 0));
      ASSERT_EQ(sent_txn->internal().involved_partitions(3), MakeMachineId(1, 0, 1));

      ASSERT_EQ(sent_txn->internal().active_partitions_size(), 2);
      ASSERT_EQ(sent_txn->internal().active_partitions(0), MakeMachineId(1, 0, 0));
      ASSERT_EQ(sent_txn->internal().active_partitions(1), MakeMachineId(1, 0, 1));

      ASSERT_EQ(sent_txn->internal().involved_regions_size(), 2);
      ASSERT_EQ(sent_txn->internal().involved_regions(0), 0);
      ASSERT_EQ(sent_txn->internal().involved_regions(1), 1);

      ASSERT_EQ(sent_txn->keys_size(), 2);
      ASSERT_EQ(TxnValueEntry(*sent_txn, "1"), TxnValueEntry(txn, "1"));
      ASSERT_EQ(TxnValueEntry(*sent_txn, "3"), TxnValueEntry(txn, "3"));

      ASSERT_TRUE(homes.count(sent_txn->internal().home()) == 0);
      ASSERT_TRUE(sent_txn->internal().home() == 0 || sent_txn->internal().home() == 1);
      homes.insert(sent_txn->internal().home());
    }
  }

}

int main(int argc, char* argv[]) {
  FLAGS_v = 4;

  ::testing::InitGoogleTest(&argc, argv);
  google::InstallFailureSignalHandler();
  return RUN_ALL_TESTS();
}