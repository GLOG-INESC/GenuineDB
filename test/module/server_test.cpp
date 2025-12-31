#include "module/forwarder.h"

#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "common/configuration.h"
#include "common/constants.h"
#include "module/server.h"
#include "proto/api.pb.h"
#include "test/test_utils.h"

using namespace std;
using namespace slog;

class ServerTest : public ::testing::Test {
 protected:
  static const size_t NUM_MACHINES = 1;

  void SetUp() {
    configs = MakeTestConfigurations("server", 1 /* num_regions */, 1 /* num_replicas */, 1 /* num_partitions */);

    for (size_t i = 0; i < NUM_MACHINES; i++) {
      test_geologs[i] = make_unique<TestSlog>(configs[i]);
      test_geologs[i]->AddServerAndClient();
      test_geologs[i]->AddOutputSocket(kForwarderChannel);
      senders_[i] = test_geologs[i]->NewSender();
    }

    for (const auto& test_slog : test_geologs) {
      test_slog->StartInNewThreads();
    }
  }

  slog::internal::Envelope* ReceiveOnForwardChannel(MachineId id) {
    auto req_env = test_geologs[id]->ReceiveFromOutputSocket(kForwarderChannel);
    if (req_env == nullptr) {
      return nullptr;
    }

    return req_env.release();
  }

  void SendToServer(MachineId from, MachineId to, const slog::internal::Envelope& req, int home) {
    auto it = senders_.find(from);
    CHECK(it != senders_.end());
    it->second->Send(req, to, kServerChannel);
  }

  unique_ptr<TestSlog> test_geologs[NUM_MACHINES];
  unordered_map<MachineId, unique_ptr<Sender>> senders_;
  ConfigVec configs;

 private:
  Transaction* ExtractTxn(EnvelopePtr& req) {
    if (req->request().type_case() != internal::Request::kForwardTxn) {
      return nullptr;
    }
    return req->mutable_request()->mutable_forward_txn()->release_txn();
  }
};

TEST_F(ServerTest, SendTxnToForward){

  auto txn = MakeTestTransaction(configs[0], 1000,
                                 {{"A", KeyType::READ, 0}, {"B", KeyType::READ, 0}, {"C", KeyType::WRITE, 0}});
  auto expected_txn = *txn;

  test_geologs[0]->SendTxn(txn);
  auto envelope = ReceiveOnForwardChannel(0);
  ASSERT_EQ(internal::Request::kForwardTxn, envelope->request().type_case());

  auto forwarded_txn = envelope->mutable_request()->release_forward_txn();
  auto received_txn = forwarded_txn->release_txn();
  ASSERT_EQ(TXN_ID(test_geologs[0]->config()->local_machine_id(), 1), received_txn->internal().client_id());
  ASSERT_EQ(TransactionStatus::NOT_STARTED, received_txn->status());
  ASSERT_EQ(received_txn->internal().type(), TransactionType::UNKNOWN);
  ASSERT_EQ(TxnValueEntry(expected_txn, "A"), TxnValueEntry(*received_txn, "A"));
  ASSERT_EQ(TxnValueEntry(expected_txn, "B"), TxnValueEntry(*received_txn, "B"));
  ASSERT_EQ(TxnValueEntry(expected_txn, "C"), TxnValueEntry(*received_txn, "C"));
}