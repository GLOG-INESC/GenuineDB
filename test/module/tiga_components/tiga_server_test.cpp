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
using slog::TxnValueEntry;

class TigaServerTest : public ::testing::Test {
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

    configs = MakeTigaTestConfigurations("tiga_server", 2 /* num_regions */, 1 /* num_replicas */, 1 /* num_partitions */, extra_config);

    // We isolate a single acceptor
    for (size_t i = 0; i < NUM_MACHINES; i++) {
      test_tiga[i] = std::make_unique<TestTiga>(configs[i], true);
      test_tiga[i]->AddServerAndClient();
      test_tiga[i]->AddOutputSocket(slog::kForwarderChannel);
      senders_[i] = test_tiga[i]->NewSender();
    }

    for (const auto& tiga : test_tiga) {
      tiga->StartTigaInNewThreads();
    }
  }

  EnvelopePtr ReceiveOnCoordinatorChannel(size_t machine) {
    return test_tiga[machine]->ReceiveFromOutputSocket(slog::kForwarderChannel, true);
  }

  void SendToServer(int from, MachineId to, EnvelopePtr&& req) {
    senders_[from]->Send(std::move(req), to, slog::kServerChannel);
  }

  ConfigVec configs;
  unique_ptr<TestTiga> test_tiga[NUM_MACHINES];
  unique_ptr<Sender> senders_[NUM_MACHINES];
private:
  Transaction* ExtractTxn(EnvelopePtr& req) {
    if (req->request().type_case() != slog::internal::Request::kForwardTxn) {
      return nullptr;
    }
    return req->mutable_request()->mutable_forward_txn()->release_txn();
  }
};

TEST_F(TigaServerTest, SendAndReceiveSpeculativeSingleHomeTxn) {

  auto txn = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::READ, 0}});
  auto txn_sent = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::READ, 0}});

  test_tiga[0]->SendTxn(txn_sent);

  // Ensure the transaction gets to the coordinator
  auto envelope = ReceiveOnCoordinatorChannel(0);

  ASSERT_EQ(slog::internal::Request::kForwardTxn, envelope->request().type_case());

  auto forwarded_txn = envelope->mutable_request()->release_forward_txn();
  auto received_txn = forwarded_txn->release_txn();
  ASSERT_EQ(received_txn->status(), slog::TransactionStatus::NOT_STARTED);
  ASSERT_EQ(received_txn->internal().type(), slog::TransactionType::UNKNOWN);
  ASSERT_EQ(TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn, "0"));

  // Issue the tiga txn confirmation

  auto env = std::make_unique<slog::internal::Envelope>();
  auto tiga_finished = env->mutable_request()->mutable_tiga_finished_subtxn();

  tiga_finished->set_timestamp(100);
  tiga_finished->mutable_finished_subtxn()->mutable_txn()->CopyFrom(*txn);
  tiga_finished->mutable_finished_subtxn()->mutable_txn()->mutable_internal()->set_id(received_txn->internal().id());
  SendToServer(0, 0, std::move(env));

  auto final_txn = test_tiga[0]->RecvTxnResult();
  ASSERT_EQ(TxnValueEntry(final_txn, "0"), TxnValueEntry(*txn, "0"));
}

TEST_F(TigaServerTest, SendAndReceiveSpeculativeMultiHomeTxn) {

  auto txn = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::READ, 0}, {"1", KeyType::READ, 1}});
  auto txn_sent = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::READ, 0}, {"1", KeyType::READ, 1}});

  test_tiga[0]->SendTxn(txn_sent);

  // Ensure the transaction gets to the coordinator
  auto envelope = ReceiveOnCoordinatorChannel(0);

  ASSERT_EQ(slog::internal::Request::kForwardTxn, envelope->request().type_case());

  auto forwarded_txn = envelope->mutable_request()->release_forward_txn();
  auto received_txn = forwarded_txn->release_txn();
  ASSERT_EQ(received_txn->status(), slog::TransactionStatus::NOT_STARTED);
  ASSERT_EQ(received_txn->internal().type(), slog::TransactionType::UNKNOWN);
  ASSERT_EQ(TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn, "0"));

  // Issue the tiga txn confirmation from server 0
  {

  auto env = std::make_unique<slog::internal::Envelope>();
  auto tiga_finished = env->mutable_request()->mutable_tiga_finished_subtxn();

  tiga_finished->set_timestamp(100);
  tiga_finished->mutable_finished_subtxn()->mutable_txn()->CopyFrom(*txn);
  tiga_finished->mutable_finished_subtxn()->mutable_txn()->mutable_internal()->set_id(received_txn->internal().id());
  SendToServer(0, 0, std::move(env));

    sleep(2);
  auto final_txn = test_tiga[0]->RecvTxnResultTimeout();
  ASSERT_FALSE(final_txn.has_internal());

  // Issue from the the other server

  }

  auto env = std::make_unique<slog::internal::Envelope>();
  auto tiga_finished = env->mutable_request()->mutable_tiga_finished_subtxn();

  tiga_finished->set_timestamp(100);
  tiga_finished->mutable_finished_subtxn()->mutable_txn()->CopyFrom(*txn);
  tiga_finished->mutable_finished_subtxn()->mutable_txn()->mutable_internal()->set_id(received_txn->internal().id());
  SendToServer(1, 0, std::move(env));

  sleep(2);
  auto final_txn = test_tiga[0]->RecvTxnResult();
  ASSERT_TRUE(final_txn.has_internal());
  ASSERT_EQ(TxnValueEntry(final_txn, "0"), TxnValueEntry(*txn, "0"));
  ASSERT_EQ(TxnValueEntry(final_txn, "1"), TxnValueEntry(*txn, "1"));

}

TEST_F(TigaServerTest, SendWrongSpeculativeMultiHomeTxn) {

  auto txn = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::READ, 0}, {"1", KeyType::READ, 1}});
  auto txn_sent = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::READ, 0}, {"1", KeyType::READ, 1}});

  test_tiga[0]->SendTxn(txn_sent);

  // Ensure the transaction gets to the coordinator
  auto envelope = ReceiveOnCoordinatorChannel(0);

  ASSERT_EQ(slog::internal::Request::kForwardTxn, envelope->request().type_case());

  auto forwarded_txn = envelope->mutable_request()->release_forward_txn();
  auto received_txn = forwarded_txn->release_txn();
  ASSERT_EQ(received_txn->status(), slog::TransactionStatus::NOT_STARTED);
  ASSERT_EQ(received_txn->internal().type(), slog::TransactionType::UNKNOWN);
  ASSERT_EQ(TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn, "0"));

  // Issue the tiga txn confirmation from server 0
  {

  auto env = std::make_unique<slog::internal::Envelope>();
  auto tiga_finished = env->mutable_request()->mutable_tiga_finished_subtxn();

  tiga_finished->set_timestamp(100);
  tiga_finished->mutable_finished_subtxn()->mutable_txn()->CopyFrom(*txn);
  auto issuing_txn =  tiga_finished->mutable_finished_subtxn()->mutable_txn();
  issuing_txn->mutable_internal()->set_id(received_txn->internal().id());
  SendToServer(0, 0, std::move(env));

    sleep(2);
  auto final_txn = test_tiga[0]->RecvTxnResultTimeout();
  ASSERT_FALSE(final_txn.has_internal());

  }

  // Issue Again, now with a higher timestamp so it must reset
  {
    auto env = std::make_unique<slog::internal::Envelope>();
    auto tiga_finished = env->mutable_request()->mutable_tiga_finished_subtxn();

    tiga_finished->set_timestamp(200);
    tiga_finished->mutable_finished_subtxn()->mutable_txn()->CopyFrom(*txn);
    tiga_finished->mutable_finished_subtxn()->mutable_txn()->mutable_internal()->set_id(received_txn->internal().id());
    SendToServer(0, 0, std::move(env));

    sleep(2);
    auto final_txn = test_tiga[0]->RecvTxnResultTimeout();
    ASSERT_FALSE(final_txn.has_internal());
  }

  // Issue From second place, it should return
  {
    auto env = std::make_unique<slog::internal::Envelope>();
    auto tiga_finished = env->mutable_request()->mutable_tiga_finished_subtxn();

    tiga_finished->set_timestamp(200);
    tiga_finished->mutable_finished_subtxn()->mutable_txn()->CopyFrom(*txn);
    tiga_finished->mutable_finished_subtxn()->mutable_txn()->mutable_internal()->set_id(received_txn->internal().id());
    SendToServer(1, 0, std::move(env));

    sleep(2);
    auto final_txn = test_tiga[0]->RecvTxnResult();
    ASSERT_TRUE(final_txn.has_internal());
    ASSERT_EQ(TxnValueEntry(final_txn, "0"), TxnValueEntry(*txn, "0"));
    ASSERT_EQ(TxnValueEntry(final_txn, "1"), TxnValueEntry(*txn, "1"));
  }



}
