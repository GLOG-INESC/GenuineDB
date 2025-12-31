#include <gtest/gtest.h>

#include "common/bitmap.h"
#include "common/proto_utils.h"
#include "module/scheduler_components/worker.h"
#include "test/test_utils.h"
#include "test/tiga_test_utils.h"

using namespace tiga;

using slog::ConfigVec;
using slog::Sender;
using slog::EnvelopePtr;
using slog::MakeTransaction;
using slog::KeyType;
using slog::internal::Envelope;

class SchedulerTest : public ::testing::Test {
 protected:
  static const size_t NUM_MACHINES = 2;

  void SetUp() {
    Configuration extra_config;
    extra_config.mutable_simple_partitioning()->set_num_records(2500000);
    extra_config.mutable_simple_partitioning()->set_record_size_bytes(100);
    extra_config.set_num_workers(1);

    configs = MakeTigaTestConfigurations("scheduler", 2 /* num_regions */, 1 /* num_replicas */, 1 /* num_partitions */, extra_config);

    // We isolate a single acceptor
    for (size_t i = 0; i < NUM_MACHINES; i++) {
      test_tiga[i] = std::make_unique<TestTiga>(configs[i], true);
      test_tiga[i]->AddScheduler();
      senders_[i] = test_tiga[i]->NewSender();
    }

    for (const auto& tiga : test_tiga) {
      tiga->StartTigaInNewThreads();
    }

    // After initializing the schedulers, connect the workser sockets to them
    for (size_t i = 0; i < NUM_MACHINES; i++) {
      test_tiga[i]->AddWorkerSocketOutput();
    }
  }

  zmq::message_t ReceiveOnWorkerChannel(size_t machine, int worker_id, int wait = 0) {
    return test_tiga[machine]->ReceiveFromWorkerOutputSocket(worker_id, milliseconds(wait));
  }

  void SendToScheduler(int i, EnvelopePtr&& req){
    senders_[i]->Send(std::move(req), slog::kSchedulerChannel);
  }

  void WorkerSendToScheduler(int i, int worker_id, zmq::message_t && msg){
    test_tiga[i]->SendMessageThroughWorkerSocket(worker_id, std::move(msg));
  }

  ConfigVec configs;
  unique_ptr<TestTiga> test_tiga[NUM_MACHINES];
  unique_ptr<Sender> senders_[NUM_MACHINES];

};

TEST_F(SchedulerTest, IssueSpecExecution) {

  auto txn = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::READ, 0}});

  auto env = std::make_unique<slog::internal::Envelope>();

  auto spec_execution = env->mutable_request()->mutable_spec_execution();
  spec_execution->set_speculative(true);
  spec_execution->set_timestamp(10);
  spec_execution->set_txn_id(1000);
  spec_execution->mutable_txn()->CopyFrom(*txn);

  SendToScheduler(0, std::move(env));

  auto response = ReceiveOnWorkerChannel(0, 0);
  auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

  ASSERT_EQ(10, timestamp);
  ASSERT_FALSE(definitive);
  ASSERT_FALSE(rollback_flag);
  ASSERT_EQ(received_txn->internal().id(), 1000);
  ASSERT_EQ(received_txn->keys_size(), 1);
  ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn, "0"));
}

TEST_F(SchedulerTest, IssueConflictTransactionAfterExecution) {

  auto txn_1 = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::WRITE, 0}});
  auto txn_2 = slog::MakeTestTransaction(configs[0], 2000, {{"0", KeyType::WRITE, 0}});

  auto txn_1_ts = 1000;
  auto txn_2_ts = 2000;
  // Issue first transaction
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(txn_1_ts);
    spec_execution->set_txn_id(1000);
    spec_execution->mutable_txn()->CopyFrom(*txn_1);

    SendToScheduler(0, std::move(env));

    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

    ASSERT_EQ(txn_1_ts, timestamp);
    ASSERT_FALSE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), 1000);
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_1, "0"));
  }

  // Issue Second transaction
  // Because the transaction conflicts, it should not be issued to the worker channel

  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(txn_2_ts);
    spec_execution->set_txn_id(2000);
    spec_execution->mutable_txn()->CopyFrom(*txn_2);

    SendToScheduler(0, std::move(env));

    auto response = ReceiveOnWorkerChannel(0, 0, 500);

    ASSERT_EQ(response.size(), 0);
  }

  // Server now gets both the Agreement confirmation and Execution confirmation, so it will release the locks
  {
    auto env = std::make_unique<slog::internal::Envelope>();
    auto spec_confirmation = env->mutable_request()->mutable_spec_confirmation();

    spec_confirmation->set_txn_id(txn_1->internal().id());
    spec_confirmation->set_spec_confirmation(true);

    SendToScheduler(0, std::move(env));

    // This should not be enough to release txn 2
    auto response = ReceiveOnWorkerChannel(0, 0, 500);

    ASSERT_EQ(response.size(), 0);
  }
  // Issue Execution confirmation
  {
    zmq::message_t msg(sizeof(std::pair<TxnId, bool>));
    *msg.data<std::pair<TxnId, bool>>() = std::make_pair(txn_1->internal().id(), false);
    WorkerSendToScheduler(0, 0, std::move(msg));

    // After execution confirmation, txn 2 should finally be issued
    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

    ASSERT_EQ(txn_2_ts, timestamp);
    ASSERT_FALSE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_2->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_2, "0"));
  }
}

TEST_F(SchedulerTest, ConflictTransactionExecutionBeforeAgreement) {

  auto txn_1 = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::WRITE, 0}});
  auto txn_2 = slog::MakeTestTransaction(configs[0], 2000, {{"0", KeyType::WRITE, 0}});

  auto txn_1_ts = 1000;
  auto txn_2_ts = 2000;
  // Issue first transaction
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(txn_1_ts);
    spec_execution->set_txn_id(1000);
    spec_execution->mutable_txn()->CopyFrom(*txn_1);

    SendToScheduler(0, std::move(env));

    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

    ASSERT_EQ(txn_1_ts, timestamp);
    ASSERT_FALSE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), 1000);
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_1, "0"));
  }

  // Issue Second transaction
  // Because the transaction conflicts, it should not be issued to the worker channel

  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(txn_2_ts);
    spec_execution->set_txn_id(2000);
    spec_execution->mutable_txn()->CopyFrom(*txn_2);

    SendToScheduler(0, std::move(env));

    auto response = ReceiveOnWorkerChannel(0, 0, 500);

    ASSERT_EQ(response.size(), 0);
  }

  // Issue Execution confirmation
  {
    zmq::message_t msg(sizeof(std::pair<TxnId, bool>));
    *msg.data<std::pair<TxnId, bool>>() = std::make_pair(txn_1->internal().id(), false);
    WorkerSendToScheduler(0, 0, std::move(msg));

    // After execution confirmation, txn 2 should finally be issued
    auto response = ReceiveOnWorkerChannel(0, 0, 500);
    ASSERT_EQ(response.size(), 0);

  }

  // Server now gets both the Agreement confirmation and Execution confirmation, so it will release the locks
  {
    auto env = std::make_unique<slog::internal::Envelope>();
    auto spec_confirmation = env->mutable_request()->mutable_spec_confirmation();

    spec_confirmation->set_txn_id(txn_1->internal().id());
    spec_confirmation->set_spec_confirmation(true);

    SendToScheduler(0, std::move(env));

    // This should not be enough to release txn 2
    auto response = ReceiveOnWorkerChannel(0, 0);

    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

    ASSERT_EQ(txn_2_ts, timestamp);
    ASSERT_FALSE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_2->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_2, "0"));
  }

}

TEST_F(SchedulerTest, IssueDefinitiveExecution) {

  auto txn = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::READ, 0}});

  auto env = std::make_unique<slog::internal::Envelope>();

  auto spec_execution = env->mutable_request()->mutable_spec_execution();
  spec_execution->set_speculative(false);
  spec_execution->set_timestamp(10);
  spec_execution->set_txn_id(1000);
  spec_execution->mutable_txn()->CopyFrom(*txn);

  SendToScheduler(0, std::move(env));

  auto response = ReceiveOnWorkerChannel(0, 0);
  auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

  ASSERT_EQ(10, timestamp);
  ASSERT_TRUE(definitive);
  ASSERT_FALSE(rollback_flag);
  ASSERT_EQ(received_txn->internal().id(), 1000);
  ASSERT_EQ(received_txn->keys_size(), 1);
  ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn, "0"));

}

TEST_F(SchedulerTest, IssueConflictTransactionWithDefinitive) {
  // If txn_1 is definitive, then conflicting transaction txn_2 should be executed right after its execution result,
  // no need for agreement message
  auto txn_1 = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::WRITE, 0}});
  auto txn_2 = slog::MakeTestTransaction(configs[0], 2000, {{"0", KeyType::WRITE, 0}});

  auto txn_1_ts = 1000;
  auto txn_2_ts = 2000;
  // Issue first transaction
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(false);
    spec_execution->set_timestamp(txn_1_ts);
    spec_execution->set_txn_id(1000);
    spec_execution->mutable_txn()->CopyFrom(*txn_1);

    SendToScheduler(0, std::move(env));

    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

    ASSERT_EQ(txn_1_ts, timestamp);
    ASSERT_TRUE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), 1000);
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_1, "0"));
  }

  // Issue Second transaction
  // Because the transaction conflicts, it should not be issued to the worker channel

  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(txn_2_ts);
    spec_execution->set_txn_id(2000);
    spec_execution->mutable_txn()->CopyFrom(*txn_2);

    SendToScheduler(0, std::move(env));

    auto response = ReceiveOnWorkerChannel(0, 0, 500);

    ASSERT_EQ(response.size(), 0);
  }

  // Server now gets the Execution result, and as such should release the next transaction
  {
    zmq::message_t msg(sizeof(std::pair<TxnId, bool>));
    *msg.data<std::pair<TxnId, bool>>() = std::make_pair(txn_1->internal().id(), false);
    WorkerSendToScheduler(0, 0, std::move(msg));

    // After execution confirmation, txn 2 should finally be issued
    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

    ASSERT_EQ(txn_2_ts, timestamp);
    ASSERT_FALSE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_2->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_2, "0"));
  }
}

/* Wrong Speculation Tests */

/* After issuing the transaction */
TEST_F(SchedulerTest, WrongSpeculationAfterIssuingBeforeResponse) {
  auto txn_1 = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::WRITE, 0}});

  auto speculative_ts = 1000;
  auto definitive_ts = 2000;

  // Issue first transaction
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(speculative_ts);
    spec_execution->set_txn_id(1000);
    spec_execution->mutable_txn()->CopyFrom(*txn_1);

    SendToScheduler(0, std::move(env));

    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

    ASSERT_EQ(timestamp, speculative_ts);
    ASSERT_FALSE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_1->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_1, "0"));
  }


  // Meanwhile, Scheduler receives confirmation that the issued transaction was wrongfully speculated
  // Because it had issued and had not gotten a response yet, ignore the transaction's next response and issue
  // a rollback
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(false);
    spec_execution->set_timestamp(definitive_ts);
    spec_execution->set_txn_id(txn_1->internal().id());

    SendToScheduler(0, std::move(env));
  }

  // Get the rollback request and issue rollback confirmation back to the scheduler
  {
    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();
    ASSERT_EQ(timestamp, speculative_ts);
    ASSERT_TRUE(definitive);
    ASSERT_TRUE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_1->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_1, "0"));

    zmq::message_t msg(sizeof(std::pair<TxnId, bool>));
    *msg.data<std::pair<TxnId, bool>>() = std::make_pair(txn_1->internal().id(), true);
    WorkerSendToScheduler(0, 0, std::move(msg));
  }

  // After receiving the rollback confirmation, the scheduler should now reissue the txn
  // Get the rollback request and issue rollback confirmation back to the scheduler
  {
    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();
    ASSERT_EQ(timestamp, definitive_ts);
    ASSERT_TRUE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_1->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_1, "0"));
  }
}

// After issuing the rollback, the scheduler may receive the txn result
// Ensure it still issues the rollback and everything as expected
TEST_F(SchedulerTest, WrongSpeculationAfterIssuingBeforeResponse2) {
  auto txn_1 = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::WRITE, 0}});

  auto speculative_ts = 1000;
  auto definitive_ts = 2000;

  // Issue first transaction
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(speculative_ts);
    spec_execution->set_txn_id(1000);
    spec_execution->mutable_txn()->CopyFrom(*txn_1);

    SendToScheduler(0, std::move(env));

    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

    ASSERT_EQ(timestamp, speculative_ts);
    ASSERT_FALSE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_1->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_1, "0"));
  }


  // Meanwhile, Scheduler receives confirmation that the issued transaction was wrongfully speculated
  // Because it had issued and had not gotten a response yet, ignore the transaction's next response and issue
  // a rollback
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(false);
    spec_execution->set_timestamp(definitive_ts);
    spec_execution->set_txn_id(txn_1->internal().id());

    SendToScheduler(0, std::move(env));
  }

  // Get the rollback request
  {
    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();
    ASSERT_EQ(timestamp, speculative_ts);
    ASSERT_TRUE(definitive);
    ASSERT_TRUE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_1->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_1, "0"));
  }

  // After issuing the rollback, the server gets the response for the original speculation
  // It should not affect the rest of the protocol nor free any locks
  {
    zmq::message_t msg(sizeof(std::pair<TxnId, bool>));
    *msg.data<std::pair<TxnId, bool>>() = std::make_pair(txn_1->internal().id(), false);
    WorkerSendToScheduler(0, 0, std::move(msg));

    auto response = ReceiveOnWorkerChannel(0, 0, 500);
    ASSERT_EQ(response.size(), 0);
  }
  // Issue rollback confirmation back to the scheduler
  {
    zmq::message_t msg(sizeof(std::pair<TxnId, bool>));
    *msg.data<std::pair<TxnId, bool>>() = std::make_pair(txn_1->internal().id(), true);
    WorkerSendToScheduler(0, 0, std::move(msg));
  }

  // After receiving the rollback confirmation, the scheduler should now reissue the txn
  // Get the rollback request and issue rollback confirmation back to the scheduler
  {
    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();
    ASSERT_EQ(timestamp, definitive_ts);
    ASSERT_TRUE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_1->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_1, "0"));
  }
}

TEST_F(SchedulerTest, WrongSpeculationAfterIssuingAfterResponse) {
  auto txn_1 = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::WRITE, 0}});

  auto speculative_ts = 1000;
  auto definitive_ts = 2000;

  // Issue first transaction
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(speculative_ts);
    spec_execution->set_txn_id(1000);
    spec_execution->mutable_txn()->CopyFrom(*txn_1);

    SendToScheduler(0, std::move(env));

    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

    ASSERT_EQ(timestamp, speculative_ts);
    ASSERT_FALSE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_1->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_1, "0"));
  }

  // The worker finishes the transaction and responds to the Scheduler
  {
    zmq::message_t msg(sizeof(std::pair<TxnId, bool>));
    *msg.data<std::pair<TxnId, bool>>() = std::make_pair(txn_1->internal().id(), false);
    WorkerSendToScheduler(0, 0, std::move(msg));
  }

  // After, the scheduler gets confirmation that the speculated transaction was indeed wrong, so it issues for a
  // rollback and then issues the transaction again
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(false);
    spec_execution->set_timestamp(definitive_ts);
    spec_execution->set_txn_id(txn_1->internal().id());

    SendToScheduler(0, std::move(env));
  }

  // Get the rollback request and issue rollback confirmation back to the scheduler
  {
    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();
    ASSERT_EQ(timestamp, speculative_ts);
    ASSERT_TRUE(definitive);
    ASSERT_TRUE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_1->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_1, "0"));

    zmq::message_t msg(sizeof(std::pair<TxnId, bool>));
    *msg.data<std::pair<TxnId, bool>>() = std::make_pair(txn_1->internal().id(), true);
    WorkerSendToScheduler(0, 0, std::move(msg));
  }

  // After receiving the rollback confirmation, the scheduler should now reissue the txn
  // Get the rollback request and issue rollback confirmation back to the scheduler
  {
    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();
    ASSERT_EQ(timestamp, definitive_ts);
    ASSERT_TRUE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_1->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_1, "0"));
  }
}

TEST_F(SchedulerTest, WrongSpeculationWithConcurrentTxn) {
  auto txn_1 = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::WRITE, 0}});
  auto txn_2 = slog::MakeTestTransaction(configs[0], 2000, {{"0", KeyType::WRITE, 0}});

  auto speculative_ts = 1000;
  auto speculative_ts_2 = 1500;
  auto definitive_ts = 2000;

  // Issue first transaction
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(speculative_ts);
    spec_execution->set_txn_id(txn_1->internal().id());
    spec_execution->mutable_txn()->CopyFrom(*txn_1);

    SendToScheduler(0, std::move(env));

    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

    ASSERT_EQ(timestamp, speculative_ts);
    ASSERT_FALSE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_1->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_1, "0"));
  }

  // Issue second transaction
  // Because it is concurrent, it wont trigger and issuing from scheduler
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(speculative_ts_2);
    spec_execution->set_txn_id(txn_2->internal().id());
    spec_execution->mutable_txn()->CopyFrom(*txn_2);

    SendToScheduler(0, std::move(env));

    auto response = ReceiveOnWorkerChannel(0, 0, 500);
    ASSERT_EQ(response.size(), 0);
  }

  // Meanwhile, Scheduler receives confirmation that the issued transaction was wrongfully speculated
  // Because it had issued and had not gotten a response yet, ignore the transaction's next response and issue
  // a rollback
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(false);
    spec_execution->set_timestamp(definitive_ts);
    spec_execution->set_txn_id(txn_1->internal().id());

    SendToScheduler(0, std::move(env));
  }

  // Get the rollback request and issue rollback confirmation back to the scheduler
  {
    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();
    ASSERT_EQ(timestamp, speculative_ts);
    ASSERT_TRUE(definitive);
    ASSERT_TRUE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_1->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_1, "0"));

    zmq::message_t msg(sizeof(std::pair<TxnId, bool>));
    *msg.data<std::pair<TxnId, bool>>() = std::make_pair(txn_1->internal().id(), true);
    WorkerSendToScheduler(0, 0, std::move(msg));
  }

  // After receiving the rollback confirmation, the scheduler should now issue txn 2, which has now a lower speculative ts
  {
    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();
    ASSERT_EQ(timestamp, speculative_ts_2);
    ASSERT_FALSE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_2->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_2, "0"));
  }
}

TEST_F(SchedulerTest, WrongSpeculationOfQueuedTxn) {
  auto txn_1 = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::WRITE, 0}});
  auto txn_2 = slog::MakeTestTransaction(configs[0], 2000, {{"0", KeyType::WRITE, 0}});

  auto speculative_ts = 1000;
  auto speculative_ts_2 = 1500;
  auto definitive_ts = 2000;

  // Issue first transaction
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(speculative_ts);
    spec_execution->set_txn_id(txn_1->internal().id());
    spec_execution->mutable_txn()->CopyFrom(*txn_1);

    SendToScheduler(0, std::move(env));

    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

    ASSERT_EQ(timestamp, speculative_ts);
    ASSERT_FALSE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_1->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_1, "0"));
  }

  // Issue second transaction
  // Because it is concurrent, it wont trigger and issuing from scheduler
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(speculative_ts_2);
    spec_execution->set_txn_id(txn_2->internal().id());
    spec_execution->mutable_txn()->CopyFrom(*txn_2);

    SendToScheduler(0, std::move(env));

    auto response = ReceiveOnWorkerChannel(0, 0, 500);
    ASSERT_EQ(response.size(), 0);
  }

  // Meanwhile, Scheduler receives confirmation that the 2nd transaction was issued with incorrect speculative value
  // Because it was detected before issuing, no need for any rollback or messages to Worker
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(false);
    spec_execution->set_timestamp(definitive_ts);
    spec_execution->set_txn_id(txn_2->internal().id());

    SendToScheduler(0, std::move(env));
    auto response = ReceiveOnWorkerChannel(0, 0, 500);
    ASSERT_EQ(response.size(), 0);
  }

  // Meanwhile, Txn1 is successfully validates and Scheduler receives its response
  // So it can issue Txn2 as a definitive txn
  {
    auto env = std::make_unique<slog::internal::Envelope>();
    auto spec_confirmation = env->mutable_request()->mutable_spec_confirmation();

    spec_confirmation->set_txn_id(txn_1->internal().id());
    spec_confirmation->set_spec_confirmation(true);

    SendToScheduler(0, std::move(env));

    // This should not be enough to release txn 2
    auto response = ReceiveOnWorkerChannel(0, 0, 500);

    ASSERT_EQ(response.size(), 0);

    // Issue Execution confirmation
    zmq::message_t msg(sizeof(std::pair<TxnId, bool>));
    *msg.data<std::pair<TxnId, bool>>() = std::make_pair(txn_1->internal().id(), false);
    WorkerSendToScheduler(0, 0, std::move(msg));

  }
  {
    // After execution confirmation, txn 2 should finally be issued as definitive
    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

    ASSERT_EQ(timestamp, definitive_ts);
    ASSERT_TRUE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_2->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_2, "0"));
  }
}

TEST_F(SchedulerTest, WrongSpeculationOfQueuedTxn2) {
  auto txn_1 = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::WRITE, 0}});
  auto txn_2 = slog::MakeTestTransaction(configs[0], 2000, {{"0", KeyType::WRITE, 0}});
  auto txn_3 = slog::MakeTestTransaction(configs[0], 3000, {{"0", KeyType::WRITE, 0}});

  auto speculative_ts = 1000;
  auto speculative_ts_2 = 1500;
  auto speculative_ts_3 = 2000;

  auto definitive_ts = 3000;

  // Issue first transaction
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(speculative_ts);
    spec_execution->set_txn_id(txn_1->internal().id());
    spec_execution->mutable_txn()->CopyFrom(*txn_1);

    SendToScheduler(0, std::move(env));

    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

    ASSERT_EQ(timestamp, speculative_ts);
    ASSERT_FALSE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_1->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_1, "0"));
  }

  // Issue second transaction and third transactions
  // Because it is concurrent, it wont trigger and issuing from scheduler

  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(speculative_ts_2);
    spec_execution->set_txn_id(txn_2->internal().id());
    spec_execution->mutable_txn()->CopyFrom(*txn_2);

    SendToScheduler(0, std::move(env));

    auto response = ReceiveOnWorkerChannel(0, 0, 500);
    ASSERT_EQ(response.size(), 0);
  }

  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(false);
    spec_execution->set_timestamp(speculative_ts_3);
    spec_execution->set_txn_id(txn_3->internal().id());
    spec_execution->mutable_txn()->CopyFrom(*txn_3);

    SendToScheduler(0, std::move(env));

    auto response = ReceiveOnWorkerChannel(0, 0, 500);
    ASSERT_EQ(response.size(), 0);
  }

  // Meanwhile, Scheduler receives confirmation that the 2nd transaction was issued with incorrect speculative value
  // Because it was detected before issuing, no need for any rollback or messages to Worker
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(false);
    spec_execution->set_timestamp(definitive_ts);
    spec_execution->set_txn_id(txn_2->internal().id());

    SendToScheduler(0, std::move(env));
    auto response = ReceiveOnWorkerChannel(0, 0, 500);
    ASSERT_EQ(response.size(), 0);
  }

  // Meanwhile, Txn1 is successfully validates and Scheduler receives its response
  // It now issues Txn3, which after reordering is now before txn2
  {
    auto env = std::make_unique<slog::internal::Envelope>();
    auto spec_confirmation = env->mutable_request()->mutable_spec_confirmation();

    spec_confirmation->set_txn_id(txn_1->internal().id());
    spec_confirmation->set_spec_confirmation(true);

    SendToScheduler(0, std::move(env));

    // This should not be enough to release txn 2
    auto response = ReceiveOnWorkerChannel(0, 0, 500);

    ASSERT_EQ(response.size(), 0);

    // Issue Execution confirmation
    zmq::message_t msg(sizeof(std::pair<TxnId, bool>));
    *msg.data<std::pair<TxnId, bool>>() = std::make_pair(txn_1->internal().id(), false);
    WorkerSendToScheduler(0, 0, std::move(msg));
  }

  {
    // After execution confirmation, txn 3 should be issued as definitive
    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

    ASSERT_EQ(timestamp, speculative_ts_3);
    ASSERT_TRUE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_3->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_2, "0"));
  }

  // After Getting response from txn 3, txn 2 should be issued as definitive
  {
    // Issue Execution confirmation
    zmq::message_t msg(sizeof(std::pair<TxnId, bool>));
    *msg.data<std::pair<TxnId, bool>>() = std::make_pair(txn_3->internal().id(), false);
    WorkerSendToScheduler(0, 0, std::move(msg));

    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

    ASSERT_EQ(timestamp, definitive_ts);
    ASSERT_TRUE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_2->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_2, "0"));
  }
}

/* Lock Tests */

TEST_F(SchedulerTest, Read_Read_Lock) {
  auto txn_1 = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::READ, 0}});
  auto txn_2 = slog::MakeTestTransaction(configs[0], 2000, {{"0", KeyType::READ, 0}});

  auto speculative_ts_1 = 1000;
  auto speculative_ts_2 = 2000;

  // Issue first transaction
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(speculative_ts_1);
    spec_execution->set_txn_id(txn_1->internal().id());
    spec_execution->mutable_txn()->CopyFrom(*txn_1);

    SendToScheduler(0, std::move(env));

    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

    ASSERT_EQ(timestamp, speculative_ts_1);
    ASSERT_FALSE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_1->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_1, "0"));
  }

  // Issue Second transaction
  // Because both transactions are read-only, they should not block each other

  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(speculative_ts_2);
    spec_execution->set_txn_id(txn_2->internal().id());
    spec_execution->mutable_txn()->CopyFrom(*txn_2);

    SendToScheduler(0, std::move(env));

    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

    ASSERT_EQ(timestamp, speculative_ts_2);
    ASSERT_FALSE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_2->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_2, "0"));
  }
}

TEST_F(SchedulerTest, Read_Write_Lock) {
  auto txn_1 = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::READ, 0}});
  auto txn_2 = slog::MakeTestTransaction(configs[0], 2000, {{"0", KeyType::WRITE, 0}});
  auto txn_3 = slog::MakeTestTransaction(configs[0], 3000, {{"0", KeyType::WRITE, 0}});

  auto speculative_ts_1 = 1000;
  auto speculative_ts_2 = 2000;
  auto speculative_ts_3 = 3000;

  // Issue first transaction
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(speculative_ts_1);
    spec_execution->set_txn_id(txn_1->internal().id());
    spec_execution->mutable_txn()->CopyFrom(*txn_1);

    SendToScheduler(0, std::move(env));

    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

    ASSERT_EQ(timestamp, speculative_ts_1);
    ASSERT_FALSE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_1->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_1, "0"));
  }

  // Issue Second transaction
  // Because The second txn is write txn, it should not allow its execution
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(speculative_ts_2);
    spec_execution->set_txn_id(txn_2->internal().id());
    spec_execution->mutable_txn()->CopyFrom(*txn_2);

    SendToScheduler(0, std::move(env));

    auto response = ReceiveOnWorkerChannel(0, 0, 500);
    ASSERT_EQ(response.size(), 0);
  }

  // Similarly, the third transaction should also not be deployed
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(speculative_ts_3);
    spec_execution->set_txn_id(txn_3->internal().id());
    spec_execution->mutable_txn()->CopyFrom(*txn_3);

    SendToScheduler(0, std::move(env));

    auto response = ReceiveOnWorkerChannel(0, 0, 500);
    ASSERT_EQ(response.size(), 0);
  }

  // After confirming the correct speculation and the successful execution of the first txn, release the second one
  {
    auto env = std::make_unique<slog::internal::Envelope>();
    auto spec_confirmation = env->mutable_request()->mutable_spec_confirmation();

    spec_confirmation->set_txn_id(txn_1->internal().id());
    spec_confirmation->set_spec_confirmation(true);

    SendToScheduler(0, std::move(env));

    // Issue Execution confirmation
    zmq::message_t msg(sizeof(std::pair<TxnId, bool>));
    *msg.data<std::pair<TxnId, bool>>() = std::make_pair(txn_1->internal().id(), false);
    WorkerSendToScheduler(0, 0, std::move(msg));

    // After execution confirmation, txn 3 should be issued as definitive
    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

    ASSERT_EQ(timestamp, speculative_ts_2);
    ASSERT_FALSE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_2->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_2, "0"));

    // It should not receive txn 3 as well
    auto response_2 = ReceiveOnWorkerChannel(0, 0, 500);

    ASSERT_EQ(response_2.size(), 0);

  }

  // After confirming the correct speculation  of the second txn, release the third one
  {
    auto env = std::make_unique<slog::internal::Envelope>();
    auto spec_confirmation = env->mutable_request()->mutable_spec_confirmation();

    spec_confirmation->set_txn_id(txn_2->internal().id());
    spec_confirmation->set_spec_confirmation(true);

    SendToScheduler(0, std::move(env));

    // Issue Execution confirmation
    zmq::message_t msg(sizeof(std::pair<TxnId, bool>));
    *msg.data<std::pair<TxnId, bool>>() = std::make_pair(txn_2->internal().id(), false);
    WorkerSendToScheduler(0, 0, std::move(msg));

    // After execution confirmation, txn 3 should be issued
    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

    ASSERT_EQ(timestamp, speculative_ts_3);
    ASSERT_FALSE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_3->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_2, "0"));
  }
}

TEST_F(SchedulerTest, Write_Read_Lock) {
  auto txn_1 = slog::MakeTestTransaction(configs[0], 1000, {{"0", KeyType::WRITE, 0}});
  auto txn_2 = slog::MakeTestTransaction(configs[0], 2000, {{"0", KeyType::READ, 0}});
  auto txn_3 = slog::MakeTestTransaction(configs[0], 3000, {{"0", KeyType::READ, 0}});

  auto speculative_ts_1 = 1000;
  auto speculative_ts_2 = 2000;
  auto speculative_ts_3 = 3000;

  // Issue first transaction
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(speculative_ts_1);
    spec_execution->set_txn_id(txn_1->internal().id());
    spec_execution->mutable_txn()->CopyFrom(*txn_1);

    SendToScheduler(0, std::move(env));

    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

    ASSERT_EQ(timestamp, speculative_ts_1);
    ASSERT_FALSE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_1->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_1, "0"));
  }

  // Issue Second transaction
  // Because The second txn is write txn, it should not allow its execution
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(speculative_ts_2);
    spec_execution->set_txn_id(txn_2->internal().id());
    spec_execution->mutable_txn()->CopyFrom(*txn_2);

    SendToScheduler(0, std::move(env));

    auto response = ReceiveOnWorkerChannel(0, 0, 500);
    ASSERT_EQ(response.size(), 0);
  }

  // Similarly, the third transaction should also not be deployed
  {
    auto env = std::make_unique<slog::internal::Envelope>();

    auto spec_execution = env->mutable_request()->mutable_spec_execution();
    spec_execution->set_speculative(true);
    spec_execution->set_timestamp(speculative_ts_3);
    spec_execution->set_txn_id(txn_3->internal().id());
    spec_execution->mutable_txn()->CopyFrom(*txn_3);

    SendToScheduler(0, std::move(env));

    auto response = ReceiveOnWorkerChannel(0, 0, 500);
    ASSERT_EQ(response.size(), 0);
  }

  // After confirming the correct speculation and the successful execution of the first txn, release the second and third one
  {
    auto env = std::make_unique<slog::internal::Envelope>();
    auto spec_confirmation = env->mutable_request()->mutable_spec_confirmation();

    spec_confirmation->set_txn_id(txn_1->internal().id());
    spec_confirmation->set_spec_confirmation(true);

    SendToScheduler(0, std::move(env));

    // Issue Execution confirmation
    zmq::message_t msg(sizeof(std::pair<TxnId, bool>));
    *msg.data<std::pair<TxnId, bool>>() = std::make_pair(txn_1->internal().id(), false);
    WorkerSendToScheduler(0, 0, std::move(msg));
  }

  {
    // After execution confirmation, txn 3 should be issued as definitive
    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

    ASSERT_EQ(timestamp, speculative_ts_2);
    ASSERT_FALSE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_2->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_2, "0"));
  }

  {
    // After execution confirmation, txn 3 should be issued as definitive
    auto response = ReceiveOnWorkerChannel(0, 0);
    auto [received_txn, timestamp, definitive, rollback_flag] =  *response.data<std::tuple<Transaction*, uint64_t, bool, bool>>();

    ASSERT_EQ(timestamp, speculative_ts_3);
    ASSERT_FALSE(definitive);
    ASSERT_FALSE(rollback_flag);
    ASSERT_EQ(received_txn->internal().id(), txn_3->internal().id());
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*received_txn, "0"), TxnValueEntry(*txn_2, "0"));
  }

}
