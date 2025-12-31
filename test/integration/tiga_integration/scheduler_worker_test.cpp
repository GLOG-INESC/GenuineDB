#include <gtest/gtest.h>

#include "common/proto_utils.h"
#include "module/scheduler_components/worker.h"
#include "test/test_utils.h"
#include "test/tiga_test_utils.h"

using namespace tiga;

using slog::ConfigVec;
using slog::EnvelopePtr;
using slog::KeyType;
using slog::MakeTransaction;
using slog::Sender;
using slog::internal::Envelope;

class SchedulerWithWorkerTest : public ::testing::Test {
 protected:
  static const size_t NUM_MACHINES = 2;

  void SetUp() {
    Configuration extra_config;
    extra_config.mutable_simple_partitioning()->set_num_records(2500000);
    extra_config.mutable_simple_partitioning()->set_record_size_bytes(100);
    extra_config.set_num_workers(1);

    configs = MakeTigaTestConfigurations("scheduler", 2 /* num_regions */, 1 /* num_replicas */, 1 /* num_partitions */,
                                         extra_config);

    // We isolate a single acceptor
    for (size_t i = 0; i < NUM_MACHINES; i++) {
      test_tiga[i] = std::make_unique<TestTiga>(configs[i]);
      test_tiga[i]->AddScheduler();
      test_tiga[i]->AddOutputSocket(slog::kServerChannel);

      senders_[i] = test_tiga[i]->NewSender();
    }

    test_tiga[0]->Data("0", {"value0", 0, 1});
    test_tiga[1]->Data("1", {"value1", 1, 1});

    for (const auto& tiga : test_tiga) {
      tiga->StartTigaInNewThreads();
    }
  }

  void SendToScheduler(int i, EnvelopePtr&& req) { senders_[i]->Send(std::move(req), slog::kSchedulerChannel); }

  EnvelopePtr ReceiveOnServerChannel(size_t machine, int wait = 0) {
    if (wait != 0) {
      return test_tiga[machine]->ReceiveFromOutputSocketTimeout(slog::kServerChannel, true, milliseconds(wait));
    }
    return test_tiga[machine]->ReceiveFromOutputSocketTimeout(slog::kServerChannel);

  }

  ConfigVec configs;
  unique_ptr<TestTiga> test_tiga[NUM_MACHINES];
  unique_ptr<Sender> senders_[NUM_MACHINES];
};

TEST_F(SchedulerWithWorkerTest, IssueSpecExecution) {
  auto txn = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::WRITE, 0}}, {{"SET", "0", "New0"}});

  auto env = std::make_unique<slog::internal::Envelope>();

  auto spec_execution = env->mutable_request()->mutable_spec_execution();
  spec_execution->set_speculative(true);
  spec_execution->set_timestamp(10);
  spec_execution->set_txn_id(1000);
  spec_execution->mutable_txn()->CopyFrom(*txn);

  SendToScheduler(0, std::move(env));

  // Get response on Server with the speculative execution result
  auto response = ReceiveOnServerChannel(0);

  auto finished_tiga_sub_txn = response->mutable_request()->mutable_tiga_finished_subtxn();
  ASSERT_EQ(finished_tiga_sub_txn->timestamp(), 10);

  auto finished_txn = finished_tiga_sub_txn->mutable_finished_subtxn()->mutable_txn();
  ASSERT_EQ(finished_txn->internal().id(), txn->internal().id());
  ASSERT_EQ(finished_txn->status(), slog::TransactionStatus::COMMITTED);
  ASSERT_EQ(finished_txn->keys_size(), 1);
  ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").type(), KeyType::WRITE);
  ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").value(), "value0");
  ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").new_value(), "New0");
}

TEST_F(SchedulerWithWorkerTest, WrongSpecExecution) {
  auto txn = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::WRITE, 0}}, {{"SET", "0", "New0"}});

  auto speculative_ts = 1000;
  auto definitive_ts = 2000;

  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(speculative_ts);
    spec_execution->set_txn_id(1000);
    spec_execution->mutable_txn()->CopyFrom(*txn);

    SendToScheduler(0, std::move(env));

    // Get response on Server with the speculative execution result
    auto response = ReceiveOnServerChannel(0);

    auto finished_tiga_sub_txn = response->mutable_request()->mutable_tiga_finished_subtxn();
    ASSERT_EQ(finished_tiga_sub_txn->timestamp(), speculative_ts);

    auto finished_txn = finished_tiga_sub_txn->mutable_finished_subtxn()->mutable_txn();
    ASSERT_EQ(finished_txn->internal().id(), txn->internal().id());
    ASSERT_EQ(finished_txn->status(), slog::TransactionStatus::COMMITTED);
    ASSERT_EQ(finished_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").type(), KeyType::WRITE);
    ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").value(), "value0");
    ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").new_value(), "New0");
  }

  // Send to scheduler confirmation of wrong execution
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(false);
    spec_execution->set_timestamp(definitive_ts);
    spec_execution->set_txn_id(txn->internal().id());

    SendToScheduler(0, std::move(env));

    // Transaction is reissued for execution, Server gets new execution
    // Get response on Server with the speculative execution result
    auto response = ReceiveOnServerChannel(0);

    auto finished_tiga_sub_txn = response->mutable_request()->mutable_tiga_finished_subtxn();
    ASSERT_EQ(finished_tiga_sub_txn->timestamp(), definitive_ts);

    auto finished_txn = finished_tiga_sub_txn->mutable_finished_subtxn()->mutable_txn();
    ASSERT_EQ(finished_txn->internal().id(), txn->internal().id());
    ASSERT_EQ(finished_txn->status(), slog::TransactionStatus::COMMITTED);
    ASSERT_EQ(finished_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").type(), KeyType::WRITE);
    ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").value(), "value0");
    ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").new_value(), "New0");
  }

}

TEST_F(SchedulerWithWorkerTest, ConflictTxnWrongSpecExecution) {
  auto txn_1 = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::WRITE, 0}}, {{"SET", "0", "New0"}});
  auto txn_2 = slog::MakeTestTransaction(configs[0], 2000, {{"0", KeyType::READ, 0}}, {{"GET", "0"}});
  txn_1->mutable_internal()->set_is_one_shot_txn(true);
  txn_2->mutable_internal()->set_is_one_shot_txn(true);

  auto speculative_ts_1 = 1000;
  auto speculative_ts_2 = 2000;
  auto definitive_ts = 3000;

  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(speculative_ts_1);
    spec_execution->set_txn_id(1000);
    spec_execution->mutable_txn()->CopyFrom(*txn_1);

    SendToScheduler(0, std::move(env));

    // Get response on Server with the speculative execution result
    auto response = ReceiveOnServerChannel(0);

    auto finished_tiga_sub_txn = response->mutable_request()->mutable_tiga_finished_subtxn();
    ASSERT_EQ(finished_tiga_sub_txn->timestamp(), speculative_ts_1);

    auto finished_txn = finished_tiga_sub_txn->mutable_finished_subtxn()->mutable_txn();
    ASSERT_EQ(finished_txn->internal().id(), txn_1->internal().id());
    ASSERT_EQ(finished_txn->status(), slog::TransactionStatus::COMMITTED);
    ASSERT_EQ(finished_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").type(), KeyType::WRITE);
    ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").value(), "value0");
    ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").new_value(), "New0");
  }

  // Issue second txn, because first transaction has not been confirmed yet it must wait
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(speculative_ts_2);
    spec_execution->set_txn_id(txn_2->internal().id());
    spec_execution->mutable_txn()->CopyFrom(*txn_2);

    SendToScheduler(0, std::move(env));

    // Get response on Server with the speculative execution result
    auto response = ReceiveOnServerChannel(0);
    ASSERT_EQ(response, nullptr);
  }

  // Send to scheduler confirmation of wrong execution
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(false);
    spec_execution->set_timestamp(definitive_ts);
    spec_execution->set_txn_id(txn_1->internal().id());

    SendToScheduler(0, std::move(env));

    // Transaction 2 is now issued first and will read the original value
  }

  {
    // Transaction is reissued for execution, Server gets new execution
    // Get response on Server with the speculative execution result
    auto response = ReceiveOnServerChannel(0, 1000);

    auto finished_tiga_sub_txn = response->mutable_request()->mutable_tiga_finished_subtxn();
    ASSERT_EQ(finished_tiga_sub_txn->timestamp(), speculative_ts_2);

    auto finished_txn = finished_tiga_sub_txn->mutable_finished_subtxn()->mutable_txn();
    ASSERT_EQ(finished_txn->internal().id(), txn_2->internal().id());
    ASSERT_EQ(finished_txn->status(), slog::TransactionStatus::COMMITTED);
    ASSERT_EQ(finished_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").type(), KeyType::READ);
    ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").value(), "value0");
  }

  // Confirm timestamp of 2nd transaction, so now it issues the first

  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_confirmation = env->mutable_request()->mutable_spec_confirmation();
    spec_confirmation->set_spec_confirmation(true);
    spec_confirmation->set_txn_id(txn_2->internal().id());

    SendToScheduler(0, std::move(env));

    // Transaction is reissued for execution, Server gets new execution
    // Get response on Server with the speculative execution result
    auto response = ReceiveOnServerChannel(0, 1000);

    ASSERT_NE(response, nullptr);
    auto finished_tiga_sub_txn = response->mutable_request()->mutable_tiga_finished_subtxn();
    ASSERT_EQ(finished_tiga_sub_txn->timestamp(), definitive_ts);

    auto finished_txn = finished_tiga_sub_txn->mutable_finished_subtxn()->mutable_txn();
    ASSERT_EQ(finished_txn->internal().id(), txn_1->internal().id());
    ASSERT_EQ(finished_txn->status(), slog::TransactionStatus::COMMITTED);
    ASSERT_EQ(finished_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").type(), KeyType::WRITE);
    ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").value(), "value0");
    ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").new_value(), "New0");

  }

}
