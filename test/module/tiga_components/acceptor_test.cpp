#include <gtest/gtest.h>
#include "test/tiga_test_utils.h"
#include "test/test_utils.h"
#include "common/proto_utils.h"
#include "common/bitmap.h"

using namespace tiga;

using slog::ConfigVec;
using slog::Sender;
using slog::EnvelopePtr;
using slog::MakeTransaction;
using slog::KeyType;
using slog::internal::Envelope;

class AcceptorTest : public ::testing::Test {
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

    configs = MakeTigaTestConfigurations("acceptor", 2 /* num_regions */, 1 /* num_replicas */, 1 /* num_partitions */, extra_config);

    // We isolate a single acceptor
    for (size_t i = 0; i < NUM_MACHINES; i++) {
      test_tiga[i] = std::make_unique<TestTiga>(configs[i], true);
      if (i == 0) {
        test_tiga[i]->AddAcceptor();
      } else {
        test_tiga[i]->AddOutputSocket(slog::kSequencerChannel);
      }
      test_tiga[i]->AddOutputSocket(slog::kSchedulerChannel);
      senders_[i] = test_tiga[i]->NewSender();
    }

    for (const auto& tiga : test_tiga) {
      tiga->StartTigaInNewThreads();
    }
  }

  EnvelopePtr ReceiveOnSchedulerChannel(size_t machine) {
    return test_tiga[machine]->ReceiveFromOutputSocket(slog::kSchedulerChannel);
  }

  EnvelopePtr ReceiveOnAcceptorChannel(size_t machine) {
    return test_tiga[machine]->ReceiveFromOutputSocket(slog::kSequencerChannel, false);
  }

  void SendToAcceptor(int i, EnvelopePtr&& req){
    senders_[i]->Send(std::move(req), slog::kSequencerChannel);
  }

  ConfigVec configs;
  unique_ptr<TestTiga> test_tiga[NUM_MACHINES];
  unique_ptr<Sender> senders_[NUM_MACHINES];

};

TEST_F(AcceptorTest, SingleHomeArrivesOnTime) {

  auto txn = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::READ, 0}});

  auto env = std::make_unique<slog::internal::Envelope>();

  auto pre_accept = env->mutable_request()->mutable_tiga_pre_accept();
  pre_accept->set_proposed_timestamp(1);
  pre_accept->mutable_txn()->CopyFrom(*txn);

  SendToAcceptor(0, std::move(env));

  auto response = ReceiveOnSchedulerChannel(0);

  auto spec_execution_request = response->mutable_request()->mutable_spec_execution();
  ASSERT_EQ(spec_execution_request->timestamp(), 1);
  ASSERT_FALSE(spec_execution_request->speculative());

  auto received_txn = spec_execution_request->mutable_txn();
  ASSERT_EQ(received_txn->keys_size(), 1);
  ASSERT_EQ(received_txn->internal().type(), slog::TransactionType::SINGLE_HOME);
  ASSERT_EQ(received_txn->internal().id(), 1000);
  ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn, "0"));
  /*
  auto agreement_response = ReceiveOnAcceptorChannel(0);
  auto agreement_confirmation = agreement_response->mutable_request()->mutable_spec_confirmation();
  ASSERT_EQ(agreement_confirmation->txn_id(), 1000);
  ASSERT_TRUE(agreement_confirmation->spec_confirmation());
  */
}

TEST_F(AcceptorTest, SingleHomeArrivesLate) {

  // Issue first transaction with timestamp 100
  {
    auto txn = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::READ, 0}});

    auto env = std::make_unique<slog::internal::Envelope>();

    auto pre_accept = env->mutable_request()->mutable_tiga_pre_accept();
    pre_accept->set_proposed_timestamp(100);
    pre_accept->mutable_txn()->CopyFrom(*txn);

    SendToAcceptor(0, std::move(env));

    auto response = ReceiveOnSchedulerChannel(0);

    auto spec_execution_request = response->mutable_request()->mutable_spec_execution();

    ASSERT_EQ(spec_execution_request->timestamp(), 100);
    ASSERT_FALSE(spec_execution_request->speculative());

    auto received_txn = spec_execution_request->mutable_txn();
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(received_txn->internal().type(), slog::TransactionType::SINGLE_HOME);
    ASSERT_EQ(received_txn->internal().id(), 1000);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn, "0"));
  }

  // Issue 2nd transaction with timestamp 50
  // Because 50 < 100, the server must give it a new timestamp and reexecute it
  {
    auto txn = slog::MakeTestTransaction(configs[0], 1001, {{"0", KeyType::READ, 0}});

    auto env = std::make_unique<slog::internal::Envelope>();

    auto pre_accept = env->mutable_request()->mutable_tiga_pre_accept();
    pre_accept->set_proposed_timestamp(50);
    pre_accept->mutable_txn()->CopyFrom(*txn);

    SendToAcceptor(0, std::move(env));

    auto response = ReceiveOnSchedulerChannel(0);

    auto spec_execution_request = response->mutable_request()->mutable_spec_execution();

    ASSERT_GT(spec_execution_request->timestamp(), 100);
    ASSERT_FALSE(spec_execution_request->speculative());

    auto received_txn = spec_execution_request->mutable_txn();
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(received_txn->internal().type(), slog::TransactionType::SINGLE_HOME);
    ASSERT_EQ(received_txn->internal().id(), 1001);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn, "0"));
  }
  // Issue second transaction with timestamp 50
  // Because the second transaction

  /*
  auto agreement_response = ReceiveOnAcceptorChannel(0);
  auto agreement_confirmation = agreement_response->mutable_request()->mutable_spec_confirmation();
  ASSERT_EQ(agreement_confirmation->txn_id(), 1000);
  ASSERT_TRUE(agreement_confirmation->spec_confirmation());
  */
}

TEST_F(AcceptorTest, GlobalTransactionArrivesEarly) {
  auto txn = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::READ, 0}, {"1", KeyType::READ, 1} });

  auto env = std::make_unique<slog::internal::Envelope>();

  auto pre_accept = env->mutable_request()->mutable_tiga_pre_accept();
  pre_accept->set_proposed_timestamp(100);
  pre_accept->mutable_txn()->CopyFrom(*txn);

  SendToAcceptor(0, std::move(env));

  auto response = ReceiveOnSchedulerChannel(0);

  auto spec_execution_request = response->mutable_request()->mutable_spec_execution();

  ASSERT_EQ(spec_execution_request->timestamp(), 100);
  ASSERT_TRUE(spec_execution_request->speculative());

  auto received_txn = spec_execution_request->mutable_txn();
  ASSERT_EQ(received_txn->keys_size(), 2);
  ASSERT_EQ(received_txn->internal().type(), slog::TransactionType::MULTI_HOME_OR_LOCK_ONLY);
  ASSERT_EQ(received_txn->internal().id(), 1000);
  ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn, "0"));
  ASSERT_EQ(slog::TxnValueEntry(*received_txn, "1"), TxnValueEntry(*txn, "1"));


  auto agreement_response = ReceiveOnAcceptorChannel(1);
  auto agreement_confirmation = agreement_response->mutable_request()->mutable_timestamp_agreement();
  ASSERT_EQ(agreement_confirmation->txn_id(), 1000);
  ASSERT_EQ(agreement_confirmation->proposed_ts(), 100);
}

TEST_F(AcceptorTest, GlobalTransactionArrivesLate) {
  // Issue first transaction with timestamp 100
  {
    auto txn = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::READ, 0}});

    auto env = std::make_unique<slog::internal::Envelope>();

    auto pre_accept = env->mutable_request()->mutable_tiga_pre_accept();
    pre_accept->set_proposed_timestamp(100);
    pre_accept->mutable_txn()->CopyFrom(*txn);

    SendToAcceptor(0, std::move(env));

    auto response = ReceiveOnSchedulerChannel(0);

    auto spec_execution_request = response->mutable_request()->mutable_spec_execution();

    ASSERT_EQ(spec_execution_request->timestamp(), 100);
    ASSERT_FALSE(spec_execution_request->speculative());

    auto received_txn = spec_execution_request->mutable_txn();
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(received_txn->internal().type(), slog::TransactionType::SINGLE_HOME);
    ASSERT_EQ(received_txn->internal().id(), 1000);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn, "0"));
  }

  // Send global transaction with a smaller timestamp, now late and requires new value
  {
    auto txn = slog::MakeTestTransaction(configs[0], 1001, {{"0", KeyType::READ, 0}, {"1", KeyType::READ, 1} });

    auto env = std::make_unique<slog::internal::Envelope>();

    auto pre_accept = env->mutable_request()->mutable_tiga_pre_accept();
    pre_accept->set_proposed_timestamp(50);
    pre_accept->mutable_txn()->CopyFrom(*txn);

    SendToAcceptor(0, std::move(env));

    auto response = ReceiveOnSchedulerChannel(0);

    auto spec_execution_request = response->mutable_request()->mutable_spec_execution();

    ASSERT_GT(spec_execution_request->timestamp(), 100);
    ASSERT_TRUE(spec_execution_request->speculative());

    auto received_txn = spec_execution_request->mutable_txn();
    ASSERT_EQ(received_txn->keys_size(), 2);
    ASSERT_EQ(received_txn->internal().type(), slog::TransactionType::MULTI_HOME_OR_LOCK_ONLY);
    ASSERT_EQ(received_txn->internal().id(), 1001);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn, "0"));
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "1"), TxnValueEntry(*txn, "1"));


    auto agreement_response = ReceiveOnAcceptorChannel(1);
    auto agreement_confirmation = agreement_response->mutable_request()->mutable_timestamp_agreement();
    ASSERT_EQ(agreement_confirmation->txn_id(), 1001);
    ASSERT_GT(agreement_confirmation->proposed_ts(), 100);
  }

}

TEST_F(AcceptorTest, CorrectGlobalSpeculation) {
  auto txn = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::READ, 0}, {"1", KeyType::READ, 1} });

  auto env = std::make_unique<slog::internal::Envelope>();

  auto pre_accept = env->mutable_request()->mutable_tiga_pre_accept();
  pre_accept->set_proposed_timestamp(100);
  pre_accept->mutable_txn()->CopyFrom(*txn);

  SendToAcceptor(0, std::move(env));

  auto response = ReceiveOnSchedulerChannel(0);

  auto spec_execution_request = response->mutable_request()->mutable_spec_execution();

  ASSERT_EQ(spec_execution_request->timestamp(), 100);
  ASSERT_TRUE(spec_execution_request->speculative());

  auto received_txn = spec_execution_request->mutable_txn();
  ASSERT_EQ(received_txn->keys_size(), 2);
  ASSERT_EQ(received_txn->internal().type(), slog::TransactionType::MULTI_HOME_OR_LOCK_ONLY);
  ASSERT_EQ(received_txn->internal().id(), 1000);
  ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn, "0"));
  ASSERT_EQ(slog::TxnValueEntry(*received_txn, "1"), TxnValueEntry(*txn, "1"));


  auto agreement_response = ReceiveOnAcceptorChannel(1);
  auto agreement_confirmation = agreement_response->mutable_request()->mutable_timestamp_agreement();
  ASSERT_EQ(agreement_confirmation->txn_id(), 1000);
  ASSERT_EQ(agreement_confirmation->proposed_ts(), 100);

  // Issue agreement confirmation confirming the correct timestamp usage
  SendToAcceptor(0, std::move(agreement_response));

  auto agreement_correct_response = ReceiveOnSchedulerChannel(0);
  auto agreement_correct = agreement_correct_response->mutable_request()->mutable_spec_confirmation();

  ASSERT_EQ(agreement_correct->txn_id(), 1000);
  ASSERT_TRUE(agreement_correct->spec_confirmation());
}

// Correct Speculation Before Receiving transaction

TEST_F(AcceptorTest, TimestampAgreementBeforeTxn) {

  auto agreement_env = std::make_unique<slog::internal::Envelope>();
  auto acceptor_agreement = agreement_env->mutable_request()->mutable_timestamp_agreement();

  acceptor_agreement->set_txn_id(1000);
  acceptor_agreement->set_proposed_ts(100);

  SendToAcceptor(0, std::move(agreement_env));

  auto txn = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::READ, 0}, {"1", KeyType::READ, 1} });

  auto env = std::make_unique<slog::internal::Envelope>();

  auto pre_accept = env->mutable_request()->mutable_tiga_pre_accept();
  pre_accept->set_proposed_timestamp(100);
  pre_accept->mutable_txn()->CopyFrom(*txn);

  SendToAcceptor(0, std::move(env));

  auto response = ReceiveOnSchedulerChannel(0);

  auto spec_execution_request = response->mutable_request()->mutable_spec_execution();

  ASSERT_EQ(spec_execution_request->timestamp(), 100);
  ASSERT_TRUE(spec_execution_request->speculative());

  auto received_txn = spec_execution_request->mutable_txn();
  ASSERT_EQ(received_txn->keys_size(), 2);
  ASSERT_EQ(received_txn->internal().type(), slog::TransactionType::MULTI_HOME_OR_LOCK_ONLY);
  ASSERT_EQ(received_txn->internal().id(), 1000);
  ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn, "0"));
  ASSERT_EQ(slog::TxnValueEntry(*received_txn, "1"), TxnValueEntry(*txn, "1"));


  auto agreement_response = ReceiveOnAcceptorChannel(1);
  auto agreement_confirmation = agreement_response->mutable_request()->mutable_timestamp_agreement();
  ASSERT_EQ(agreement_confirmation->txn_id(), 1000);
  ASSERT_EQ(agreement_confirmation->proposed_ts(), 100);

  // Issue agreement confirmation confirming the correct timestamp usage
  SendToAcceptor(0, std::move(agreement_response));

  auto agreement_correct_response = ReceiveOnSchedulerChannel(0);
  auto agreement_correct = agreement_correct_response->mutable_request()->mutable_spec_confirmation();

  ASSERT_EQ(agreement_correct->txn_id(), 1000);
  ASSERT_TRUE(agreement_correct->spec_confirmation());
}

TEST_F(AcceptorTest, IncorrectSpeculationAfterIssuing) {
  auto txn = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::READ, 0}, {"1", KeyType::READ, 1} });

  auto env = std::make_unique<slog::internal::Envelope>();

  auto pre_accept = env->mutable_request()->mutable_tiga_pre_accept();
  pre_accept->set_proposed_timestamp(100);
  pre_accept->mutable_txn()->CopyFrom(*txn);

  SendToAcceptor(0, std::move(env));

  auto response = ReceiveOnSchedulerChannel(0);

  auto spec_execution_request = response->mutable_request()->mutable_spec_execution();

  ASSERT_EQ(spec_execution_request->timestamp(), 100);
  ASSERT_TRUE(spec_execution_request->speculative());

  // Proposed timestamp with shorter value
  auto agreement_env = std::make_unique<slog::internal::Envelope>();
  auto acceptor_agreement = agreement_env->mutable_request()->mutable_timestamp_agreement();

  acceptor_agreement->set_txn_id(1000);
  acceptor_agreement->set_proposed_ts(300);

  SendToAcceptor(0, std::move(agreement_env));

  auto second_response = ReceiveOnSchedulerChannel(0);
  auto final_execution_request = second_response->mutable_request()->mutable_spec_execution();

  ASSERT_EQ(final_execution_request->timestamp(), 300);
  ASSERT_EQ(final_execution_request->txn_id(), 1000);
  ASSERT_FALSE(final_execution_request->speculative());
  ASSERT_FALSE(final_execution_request->has_txn());
}
