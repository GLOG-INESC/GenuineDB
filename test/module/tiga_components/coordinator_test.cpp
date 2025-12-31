#include <gtest/gtest.h>
#include "test/tiga_test_utils.h"
#include "test/test_utils.h"
#include "module/tiga/coordinator.h"
#include "execution/tpcc/metadata_initializer.h"
#include "common/proto_utils.h"
#include "common/bitmap.h"

using namespace tiga;

using slog::ConfigVec;
using slog::Sender;
using slog::EnvelopePtr;
using slog::MakeTransaction;
using slog::KeyType;
using slog::internal::Envelope;

class CoordinatorTest : public ::testing::Test {
 protected:
  static const size_t NUM_MACHINES = 2;

  void SetUp() {
    Configuration extra_config;
    extra_config.mutable_simple_partitioning()->set_num_records(2500000);
    extra_config.mutable_simple_partitioning()->set_record_size_bytes(100);

    extra_config.add_network_latency(0.1);
    extra_config.add_network_latency(5.0);
    extra_config.add_network_latency(5.0);
    extra_config.add_network_latency(0.1);

    extra_config.set_timestamp_buffer_us(10000);

    configs = MakeTigaTestConfigurations("coordinator", 2 /* num_regions */, 1 /* num_replicas */, 1 /* num_partitions */, extra_config);

    for (size_t i = 0; i < NUM_MACHINES; i++) {
      test_tiga[i] = std::make_unique<TestTiga>(configs[i], true);
      test_tiga[i]->AddCoordinator();
      test_tiga[i]->AddOutputSocket(slog::kSequencerChannel);
      senders_[i] = test_tiga[i]->NewSender();
    }

    for (const auto& tiga : test_tiga) {
      tiga->StartTigaInNewThreads();
    }
  }

  EnvelopePtr ReceiveOnAcceptorChannel(size_t machine) {
    return test_tiga[machine]->ReceiveFromOutputSocket(slog::kSequencerChannel, false);
  }

  void SendToCoordinator(int i, EnvelopePtr&& req){
    senders_[i]->Send(std::move(req), slog::kForwarderChannel);
  }

  ConfigVec configs;
  unique_ptr<TestTiga> test_tiga[NUM_MACHINES];
  unique_ptr<Sender> senders_[NUM_MACHINES];

};

TEST_F(CoordinatorTest, IssueLocalTransaction){
  auto txn = slog::MakeTransaction({{"0"}});

  auto env = std::make_unique<slog::internal::Envelope>();
  env->mutable_request()->mutable_forward_txn()->mutable_txn()->CopyFrom(*txn);

  SendToCoordinator(0, std::move(env));

  // Region 0
  {
    auto response = ReceiveOnAcceptorChannel(0);

    auto pre_accept = response->mutable_request()->mutable_tiga_pre_accept();

    ASSERT_EQ(pre_accept->proposed_timestamp(), 100000+10000000);

    auto received_txn = pre_accept->release_txn();
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(received_txn->internal().type(), slog::TransactionType::SINGLE_HOME);
    ASSERT_EQ(received_txn->internal().id(), 1000);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn, "0"));
  }
}


TEST_F(CoordinatorTest, IssueGlobalTransaction){
  auto txn = slog::MakeTransaction({{"0"}, {"1", slog::KeyType::WRITE, 1}});

  auto env = std::make_unique<slog::internal::Envelope>();
  env->mutable_request()->mutable_forward_txn()->mutable_txn()->CopyFrom(*txn);

  SendToCoordinator(0, std::move(env));

  // Region 0
  {
    auto response = ReceiveOnAcceptorChannel(0);

    auto pre_accept = response->mutable_request()->mutable_tiga_pre_accept();

    ASSERT_EQ(pre_accept->proposed_timestamp(), 5000000+10000000);

    auto received_txn = pre_accept->release_txn();
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(received_txn->internal().type(), slog::TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(received_txn->internal().id(), 1000);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn, "0"));
  }

  // Region 1
  {
    auto response = ReceiveOnAcceptorChannel(1);

    auto pre_accept = response->mutable_request()->mutable_tiga_pre_accept();

    ASSERT_EQ(pre_accept->proposed_timestamp(), 5000000+10000000);

    auto received_txn = pre_accept->release_txn();
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(received_txn->internal().type(), slog::TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(received_txn->internal().id(), 1000);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "1"), TxnValueEntry(*txn, "1"));
  }



}