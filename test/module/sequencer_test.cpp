#include <gtest/gtest.h>

#include <vector>

#include "common/proto_utils.h"
#include "module/log_manager.h"
#include "test/test_utils.h"

using namespace std;
using namespace slog;

using internal::Envelope;
using internal::Request;

class SequencerTest : public ::testing::TestWithParam<bool> {
 public:
  void SetUp() {
    auto delayed = GetParam();
    internal::Configuration extra_config;
    extra_config.set_bypass_mh_orderer(true);
    extra_config.mutable_simple_partitioning()->set_num_records(2500000);
    extra_config.mutable_simple_partitioning()->set_record_size_bytes(100);
    if (delayed) {
      extra_config.mutable_replication_delay()->set_delay_pct(100);
      extra_config.mutable_replication_delay()->set_delay_amount_ms(5);

      extra_config.set_num_log_managers(2);
      configs_ = MakeTestConfigurations("sequencer", 2, 1, 2, extra_config);
    } else {
      configs_ = MakeTestConfigurations("sequencer", 2, 1, 2, extra_config);
    }

    for (int i = 0; i < 4; i++) {
      slog_[i] = make_unique<TestSlog>(configs_[i]);
      slog_[i]->AddSequencer();
      // There are 2 lock managers as set in the config
      slog_[i]->AddOutputSocket(kLogManagerChannel, {LogManager::MakeLogChannel(0)});
      slog_[i]->AddOutputSocket(kLogManagerChannel + 1, {LogManager::MakeLogChannel(1)});
      senders_[i] = slog_[i]->NewSender();
    }
    for (auto& slog : slog_) {
      slog->StartInNewThreads();
    }
  }

  void SendToSequencer(int i, EnvelopePtr&& req) { senders_[i]->Send(std::move(req), kSequencerChannel); }

  vector<Batch> ReceiveBatches(int i, int log_id) {
    auto req_env = slog_[i]->ReceiveFromOutputSocket(kLogManagerChannel + log_id);
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

  unique_ptr<Sender> senders_[4];
  unique_ptr<TestSlog> slog_[4];
  ConfigVec configs_;
};

TEST_P(SequencerTest, SingleHomeTransaction1) {
  // A and C are in partition 0. B is in partition 1
  auto txn = MakeTestTransaction(configs_[0], 1000,
                                 {{"0", KeyType::READ, 0}, {"1", KeyType::READ, 0}, {"4", KeyType::WRITE, 0}});

  auto env = make_unique<Envelope>();
  env->mutable_request()->mutable_forward_txn()->mutable_txn()->CopyFrom(*txn);
  SendToSequencer(0, move(env));

  // Batch partitions are distributed to corresponding local partitions
  {
    auto batches = ReceiveBatches(0, 0);
    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].transactions_size(), 1);
    ASSERT_EQ(batches[0].transaction_type(), TransactionType::SINGLE_HOME);
    auto& batched_txn = batches[0].transactions().at(0);
    ASSERT_EQ(batched_txn.internal().id(), 1000);
    ASSERT_EQ(batched_txn.keys_size(), 2);
    ASSERT_EQ(TxnValueEntry(batched_txn, "0"), TxnValueEntry(*txn, "0"));
    ASSERT_EQ(TxnValueEntry(batched_txn, "4"), TxnValueEntry(*txn, "4"));
    ASSERT_EQ(batched_txn.internal().involved_partitions_size(), 2);
  }

  {
    auto batches = ReceiveBatches(1, 0);
    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].transactions_size(), 1);
    ASSERT_EQ(batches[0].transaction_type(), TransactionType::SINGLE_HOME);
    auto& batched_txn = batches[0].transactions().at(0);
    ASSERT_EQ(batched_txn.internal().id(), 1000);
    ASSERT_EQ(batched_txn.keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(batched_txn, "1"), TxnValueEntry(*txn, "1"));
    ASSERT_EQ(batched_txn.internal().involved_partitions_size(), 2);
  }

  // All batch partitions are sent to a machine in the remote replica
  {
    auto batches = ReceiveBatches(3, 0);
    ASSERT_EQ(batches.size(), 2);
    ASSERT_EQ(batches[0].transactions_size(), 1);
    ASSERT_EQ(batches[0].transaction_type(), TransactionType::SINGLE_HOME);
    {
      auto& batched_txn = batches[0].transactions().at(0);
      ASSERT_EQ(batched_txn.internal().id(), 1000);
      ASSERT_EQ(batched_txn.keys_size(), 2);
      ASSERT_EQ(TxnValueEntry(batched_txn, "0"), TxnValueEntry(*txn, "0"));
      ASSERT_EQ(TxnValueEntry(batched_txn, "4"), TxnValueEntry(*txn, "4"));
      ASSERT_EQ(batched_txn.internal().involved_partitions_size(), 2);

    }
    ASSERT_EQ(batches[1].transactions_size(), 1);
    ASSERT_EQ(batches[1].transaction_type(), TransactionType::SINGLE_HOME);
    {
      auto& batched_txn = batches[1].transactions().at(0);
      ASSERT_EQ(batched_txn.internal().id(), 1000);
      ASSERT_EQ(batched_txn.keys_size(), 1);
      ASSERT_EQ(TxnValueEntry(batched_txn, "1"), TxnValueEntry(*txn, "1"));
      ASSERT_EQ(batched_txn.internal().involved_partitions_size(), 2);

    }
  }
}

TEST_P(SequencerTest, SingleHomeTransaction2) {
  // A and C are in partition 0. B is in partition 1
  auto txn = MakeTestTransaction(configs_[0], 1000,
                                 {{"0", KeyType::READ, 1}, {"1", KeyType::READ, 1}, {"4", KeyType::WRITE, 1}});

  auto env = make_unique<Envelope>();
  env->mutable_request()->mutable_forward_txn()->mutable_txn()->CopyFrom(*txn);
  SendToSequencer(2, move(env));

  // Batch partitions are distributed to corresponding local partitions
  {
    auto batches = ReceiveBatches(2, 1);
    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].transactions_size(), 1);
    ASSERT_EQ(batches[0].transaction_type(), TransactionType::SINGLE_HOME);
    auto& batched_txn = batches[0].transactions().at(0);
    ASSERT_EQ(batched_txn.internal().id(), 1000);
    ASSERT_EQ(batched_txn.keys_size(), 2);
    ASSERT_EQ(TxnValueEntry(batched_txn, "0"), TxnValueEntry(*txn, "0"));
    ASSERT_EQ(TxnValueEntry(batched_txn, "4"), TxnValueEntry(*txn, "4"));
  }

  {
    auto batches = ReceiveBatches(3, 1);
    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].transactions_size(), 1);
    ASSERT_EQ(batches[0].transaction_type(), TransactionType::SINGLE_HOME);
    auto& batched_txn = batches[0].transactions().at(0);
    ASSERT_EQ(batched_txn.internal().id(), 1000);
    ASSERT_EQ(batched_txn.keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(batched_txn, "1"), TxnValueEntry(*txn, "1"));
  }

  // All batch partitions are sent to a machine in the remote replica
  {
    auto batches = ReceiveBatches(1, 1);
    ASSERT_EQ(batches.size(), 2);
    ASSERT_EQ(batches[0].transactions_size(), 1);
    ASSERT_EQ(batches[0].transaction_type(), TransactionType::SINGLE_HOME);
    {
      auto& batched_txn = batches[0].transactions().at(0);
      ASSERT_EQ(batched_txn.internal().id(), 1000);
      ASSERT_EQ(batched_txn.keys_size(), 2);
      ASSERT_EQ(TxnValueEntry(batched_txn, "0"), TxnValueEntry(*txn, "0"));
      ASSERT_EQ(TxnValueEntry(batched_txn, "4"), TxnValueEntry(*txn, "4"));
    }
    ASSERT_EQ(batches[1].transactions_size(), 1);
    ASSERT_EQ(batches[1].transaction_type(), TransactionType::SINGLE_HOME);
    {
      auto& batched_txn = batches[1].transactions().at(0);
      ASSERT_EQ(batched_txn.internal().id(), 1000);
      ASSERT_EQ(batched_txn.keys_size(), 1);
      ASSERT_EQ(TxnValueEntry(batched_txn, "1"), TxnValueEntry(*txn, "1"));
    }
  }
}

TEST_P(SequencerTest, MultiHomeTransaction1) {
  //             A  B  C  D
  // Partition:  0  1  0  1
  auto txn = MakeTestTransaction(
      configs_[0], 1000,
      {{"0", KeyType::READ, 0}, {"1", KeyType::READ, 0}, {"2", KeyType::WRITE, 1}, {"3", KeyType::WRITE, 1}});
  auto env = make_unique<Envelope>();
  env->mutable_request()->mutable_forward_txn()->mutable_txn()->CopyFrom(*txn);

  SendToSequencer(0, move(env));


}

TEST_P(SequencerTest, MultiHomeTransaction2) {
  //             A  B  C  D
  // Partition:  0  1  0  1
  auto txn = MakeTestTransaction(
      configs_[0], 1000,
      {{"0", KeyType::READ, 1}, {"1", KeyType::READ, 0}, {"2", KeyType::WRITE, 1}, {"3", KeyType::WRITE, 1}});
  auto env = make_unique<Envelope>();
  env->mutable_request()->mutable_forward_txn()->mutable_txn()->CopyFrom(*txn);

  SendToSequencer(0, move(env));

  // The txn was sent to region 0, which only generates one subtxn for partition 1 because the subtxn for partition
  // 0 is redundant (both A and C are homed at region 1)
  {
    auto batches = ReceiveBatches(0, 0);
    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].transactions_size(), 0);
  }

  {
    auto batches = ReceiveBatches(1, 0);

    ASSERT_EQ(batches.size(), 1);

    auto lo_txn = batches[0].transactions().at(0);
    ASSERT_EQ(lo_txn.internal().id(), 1000);
    ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(lo_txn.internal().home(), 0);
    ASSERT_EQ(lo_txn.keys_size(), 2);
    ASSERT_EQ(TxnValueEntry(lo_txn, "1"), TxnValueEntry(*txn, "1"));
    ASSERT_EQ(TxnValueEntry(lo_txn, "3"), TxnValueEntry(*txn, "3"));
  }

  {
    auto batches = ReceiveBatches(3, 0);
    ASSERT_FALSE(batches.empty());
    ASSERT_EQ(batches.size(), 2);

    ASSERT_EQ(batches[0].transactions_size(), 0);

    auto lo_txn = batches[1].transactions().at(0);
    ASSERT_EQ(lo_txn.internal().id(), 1000);
    ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(lo_txn.internal().home(), 0);
    ASSERT_EQ(lo_txn.keys_size(), 2);
    ASSERT_EQ(TxnValueEntry(lo_txn, "1"), TxnValueEntry(*txn, "1"));
    ASSERT_EQ(TxnValueEntry(lo_txn, "3"), TxnValueEntry(*txn, "3"));
  }
}

#ifdef REMASTER_PROTOCOL_COUNTERLESS
TEST_P(SequencerTest, RemasterTransaction) {
  auto txn = MakeTestTransaction(configs_[0], 1000, {{"A", KeyType::WRITE, 1}}, {}, 0);
  auto env = make_unique<Envelope>();
  env->mutable_request()->mutable_forward_txn()->mutable_txn()->CopyFrom(*txn);

  SendToSequencer(0, move(env));

  {
    auto batches = ReceiveBatches(0, 0);

    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].transactions_size(), 1);

    auto lo_txn = batches[0].transactions().at(0);
    ASSERT_EQ(lo_txn.internal().id(), 1000);
    ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(lo_txn.internal().home(), 0);
    ASSERT_EQ(lo_txn.keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(lo_txn, "A"), TxnValueEntry(*txn, "A"));
    ASSERT_TRUE(lo_txn.remaster().is_new_master_lock_only());
  }

  {
    auto batches = ReceiveBatches(1, 0);

    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].transactions_size(), 0);
  }

  {
    auto batches = ReceiveBatches(3, 0);

    ASSERT_EQ(batches.size(), 2);
    ASSERT_EQ(batches[0].transactions_size(), 1);

    auto lo_txn = batches[0].transactions().at(0);
    ASSERT_EQ(lo_txn.internal().id(), 1000);
    ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(lo_txn.internal().home(), 0);
    ASSERT_EQ(lo_txn.keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(lo_txn, "A"), TxnValueEntry(*txn, "A"));
    ASSERT_TRUE(lo_txn.remaster().is_new_master_lock_only());

    ASSERT_EQ(batches[1].transactions_size(), 0);
  }
}
#endif

TEST_P(SequencerTest, MultiHomeTransactionBypassedOrderer) {
  // "A" and "B" are on two different partitions
  auto txn = MakeTestTransaction(configs_[0], 1000, {{"0", KeyType::READ, 0}, {"1", KeyType::READ, 1}});

  auto env = make_unique<Envelope>();
  env->mutable_request()->mutable_forward_txn()->mutable_txn()->CopyFrom(*txn);

  SendToSequencer(0, move(env));

  // The txn was sent to region 0, which only generates one subtxns for partition 0 because B of partition 1
  // does not have any key homed at 0.
  {
    auto batches = ReceiveBatches(0, 0);

    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].transactions_size(), 1);

    auto lo_txn = batches[0].transactions().at(0);
    ASSERT_EQ(lo_txn.internal().id(), 1000);
    ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(lo_txn.internal().home(), 0);
    ASSERT_EQ(lo_txn.keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(lo_txn, "0"), TxnValueEntry(*txn, "0"));
  }

  {
    auto batches = ReceiveBatches(1, 0);

    ASSERT_EQ(batches.size(), 1);
    ASSERT_EQ(batches[0].transactions_size(), 0);
  }

  {
    auto batches = ReceiveBatches(3, 0);

    ASSERT_EQ(batches.size(), 2);
    ASSERT_EQ(batches[0].transactions_size(), 1);

    auto lo_txn = batches[0].transactions().at(0);
    ASSERT_EQ(lo_txn.internal().id(), 1000);
    ASSERT_EQ(lo_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(lo_txn.internal().home(), 0);
    ASSERT_EQ(lo_txn.keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(lo_txn, "0"), TxnValueEntry(*txn, "0"));

    ASSERT_EQ(batches[1].transactions_size(), 0);
  }
}

INSTANTIATE_TEST_SUITE_P(AllSequencerTests, SequencerTest, testing::Values(false, true),
                         [](const testing::TestParamInfo<bool>& info) {
                           return info.param ? "Delayed" : "NotDelayed";
                         });