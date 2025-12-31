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

class WorkerTest : public ::testing::Test {
 protected:
  static const size_t NUM_MACHINES = 2;

  void SetUp() {
    Configuration extra_config;
    extra_config.mutable_simple_partitioning()->set_num_records(2500000);
    extra_config.mutable_simple_partitioning()->set_record_size_bytes(100);
    extra_config.set_num_workers(1);

    configs = MakeTigaTestConfigurations("worker", 2 /* num_regions */, 1 /* num_replicas */, 1 /* num_partitions */, extra_config);

    // We isolate a single acceptor
    for (size_t i = 0; i < NUM_MACHINES; i++) {
      test_tiga[i] = std::make_unique<TestTiga>(configs[i]);
      test_tiga[i]->AddWorkers();
      test_tiga[i]->AddSchedulerWorkerSocket();
      test_tiga[i]->AddOutputSocket(slog::kServerChannel);

      senders_[i] = test_tiga[i]->NewSender();
    }

    test_tiga[0]->Data("0", {"value0", 0, 1});
    test_tiga[1]->Data("1", {"value1", 1, 1});

    for (const auto& tiga : test_tiga) {
      tiga->StartTigaInNewThreads();
    }

  }

  zmq::message_t ReceiveOnSchedulerWorkerSocket(size_t machine, int worker_id, int wait = 0) {
    return test_tiga[machine]->ReceiveFromSchedulerWorkerSocket(worker_id, milliseconds(wait));
  }

  EnvelopePtr ReceiveOnServerChannel(size_t machine) {
    return test_tiga[machine]->ReceiveFromOutputSocket(slog::kServerChannel);
  }

  void SendToScheduler(int i, EnvelopePtr&& req){
    senders_[i]->Send(std::move(req), slog::kSchedulerChannel);
  }

  void SchedulerSendToWorker(int i, int worker_id, zmq::message_t && msg){
    test_tiga[i]->SendMessageThroughSchedulerWorkerSocket(worker_id, std::move(msg));
  }

  ConfigVec configs;
  unique_ptr<TestTiga> test_tiga[NUM_MACHINES];
  unique_ptr<Sender> senders_[NUM_MACHINES];

};

// One-shot Transactions
TEST_F(WorkerTest, SuccessfullSpeculativeExecution) {

  auto txn = slog::MakeTestTransaction(configs[0], 1000,
    {{"0", KeyType::WRITE, 0}},
    {{"SET", "0", "New0"}});

  txn->mutable_internal()->set_is_one_shot_txn(true);
  zmq::message_t msg(sizeof(slog::TigaIssueTxnMessage));
  *msg.data<slog::TigaIssueTxnMessage>() = std::make_tuple(txn, 100, false, false);
  SchedulerSendToWorker(0, 0, std::move(msg));

  // Get response on Server with the speculative execution result
  auto response = ReceiveOnServerChannel(0);

  auto finished_tiga_sub_txn = response->mutable_request()->mutable_tiga_finished_subtxn();
  ASSERT_EQ(finished_tiga_sub_txn->timestamp(), 100);

  auto finished_txn = finished_tiga_sub_txn->mutable_finished_subtxn()->mutable_txn();
  ASSERT_EQ(finished_txn->internal().id(), txn->internal().id());
  ASSERT_EQ(finished_txn->status(), slog::TransactionStatus::COMMITTED);
  ASSERT_EQ(finished_txn->keys_size(), 1);
  ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").type(), KeyType::WRITE);
  ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").value(), "value0");
  ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").new_value(), "New0");

  // Get response on Scheduler informing of the successfull txn speculative execution
  auto scheduler_response = ReceiveOnSchedulerWorkerSocket(0,0);

  auto [txn_id, rollback] = *scheduler_response.data<slog::TigaTxnExecutionResponse>();
  ASSERT_EQ(txn_id, txn->internal().id());
  ASSERT_FALSE(rollback);


}

TEST_F(WorkerTest, RollbackOneShotTransaction) {

  auto txn = slog::MakeTestTransaction(configs[0], 1000,
  {{"0", KeyType::WRITE, 0}},
  {{"SET", "0", "New0"}});
  txn->mutable_internal()->set_is_one_shot_txn(true);

  {
    zmq::message_t msg(sizeof(slog::TigaIssueTxnMessage));
    *msg.data<slog::TigaIssueTxnMessage>() = std::make_tuple(txn, 100, false, false);
    SchedulerSendToWorker(0, 0, std::move(msg));
  }

  {
    auto response = ReceiveOnServerChannel(0);

    auto finished_tiga_sub_txn = response->mutable_request()->mutable_tiga_finished_subtxn();
    ASSERT_EQ(finished_tiga_sub_txn->timestamp(), 100);

    auto finished_txn = finished_tiga_sub_txn->mutable_finished_subtxn()->mutable_txn();
    ASSERT_EQ(finished_txn->internal().id(), txn->internal().id());
    ASSERT_EQ(finished_txn->status(), slog::TransactionStatus::COMMITTED);
    ASSERT_EQ(finished_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").type(), KeyType::WRITE);
    ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").value(), "value0");
    ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").new_value(), "New0");

    // Get response on Scheduler informing of the successfull txn speculative execution
    auto scheduler_response = ReceiveOnSchedulerWorkerSocket(0,0);

    auto [txn_id, rollback] = *scheduler_response.data<slog::TigaTxnExecutionResponse>();
    ASSERT_EQ(txn_id, txn->internal().id());
    ASSERT_FALSE(rollback);
  }

  // Reissue transaction informing of the rollback
  {
    zmq::message_t msg(sizeof(slog::TigaIssueTxnMessage));
    *msg.data<slog::TigaIssueTxnMessage>() = std::make_tuple(txn, 100, false, true);
    SchedulerSendToWorker(0, 0, std::move(msg));
  }

  // Receive response on Scheduler informing of successfull rollback
  {
    auto scheduler_response = ReceiveOnSchedulerWorkerSocket(0,0);

    auto [txn_id, rollback] = *scheduler_response.data<slog::TigaTxnExecutionResponse>();
    ASSERT_EQ(txn_id, txn->internal().id());
    ASSERT_TRUE(rollback);
  }

  // Issue a new Read transaction, it should return the previous value

  auto read_txn = slog::MakeTestTransaction(configs[0], 2000,
    {{"0", KeyType::READ, 0}},
    {{"GET", "0"}});
  read_txn->mutable_internal()->set_is_one_shot_txn(true);

  {
    zmq::message_t msg(sizeof(slog::TigaIssueTxnMessage));
    *msg.data<slog::TigaIssueTxnMessage>() = std::make_tuple(read_txn, 200, false, false);
    SchedulerSendToWorker(0, 0, std::move(msg));
  }

  {
    auto response = ReceiveOnServerChannel(0);

    auto finished_tiga_sub_txn = response->mutable_request()->mutable_tiga_finished_subtxn();
    ASSERT_EQ(finished_tiga_sub_txn->timestamp(), 200);

    auto finished_txn = finished_tiga_sub_txn->mutable_finished_subtxn()->mutable_txn();
    ASSERT_EQ(finished_txn->internal().id(), read_txn->internal().id());
    ASSERT_EQ(finished_txn->status(), slog::TransactionStatus::COMMITTED);
    ASSERT_EQ(finished_txn->keys_size(), 1);
    ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").type(), KeyType::READ);
    ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").value(), "value0");

    // Get response on Scheduler informing of the successfull txn speculative execution
    auto scheduler_response = ReceiveOnSchedulerWorkerSocket(0,0);

    auto [txn_id, rollback] = *scheduler_response.data<slog::TigaTxnExecutionResponse>();
    ASSERT_EQ(txn_id, read_txn->internal().id());
    ASSERT_FALSE(rollback);
  }
}

TEST_F(WorkerTest, RollbackTwoOneShotTransaction) {
  // Issue First transaction
  {
    auto txn = slog::MakeTestTransaction(configs[0], 1000,
  {{"0", KeyType::WRITE, 0}},
  {{"SET", "0", "New0"}});
    txn->mutable_internal()->set_is_one_shot_txn(true);

    {
      zmq::message_t msg(sizeof(slog::TigaIssueTxnMessage));
      *msg.data<slog::TigaIssueTxnMessage>() = std::make_tuple(txn, 100, false, false);
      SchedulerSendToWorker(0, 0, std::move(msg));
    }

    {
      auto response = ReceiveOnServerChannel(0);

      auto finished_tiga_sub_txn = response->mutable_request()->mutable_tiga_finished_subtxn();
      ASSERT_EQ(finished_tiga_sub_txn->timestamp(), 100);

      auto finished_txn = finished_tiga_sub_txn->mutable_finished_subtxn()->mutable_txn();
      ASSERT_EQ(finished_txn->internal().id(), txn->internal().id());
      ASSERT_EQ(finished_txn->status(), slog::TransactionStatus::COMMITTED);
      ASSERT_EQ(finished_txn->keys_size(), 1);
      ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").type(), KeyType::WRITE);
      ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").value(), "value0");
      ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").new_value(), "New0");

      // Get response on Scheduler informing of the successfull txn speculative execution
      auto scheduler_response = ReceiveOnSchedulerWorkerSocket(0,0);

      auto [txn_id, rollback] = *scheduler_response.data<slog::TigaTxnExecutionResponse>();
      ASSERT_EQ(txn_id, txn->internal().id());
      ASSERT_FALSE(rollback);
    }
  }
  // Issue Second Transaction
  {
    auto txn = slog::MakeTestTransaction(configs[0], 2000,
  {{"0", KeyType::WRITE, 0}},
  {{"SET", "0", "New02"}});
    txn->mutable_internal()->set_is_one_shot_txn(true);

    {
      zmq::message_t msg(sizeof(slog::TigaIssueTxnMessage));
      *msg.data<slog::TigaIssueTxnMessage>() = std::make_tuple(txn, 200, false, false);
      SchedulerSendToWorker(0, 0, std::move(msg));
    }

    {
      auto response = ReceiveOnServerChannel(0);

      auto finished_tiga_sub_txn = response->mutable_request()->mutable_tiga_finished_subtxn();
      ASSERT_EQ(finished_tiga_sub_txn->timestamp(), 200);

      auto finished_txn = finished_tiga_sub_txn->mutable_finished_subtxn()->mutable_txn();
      ASSERT_EQ(finished_txn->internal().id(), txn->internal().id());
      ASSERT_EQ(finished_txn->status(), slog::TransactionStatus::COMMITTED);
      ASSERT_EQ(finished_txn->keys_size(), 1);
      ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").type(), KeyType::WRITE);
      ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").value(), "New0");
      ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").new_value(), "New02");

      // Get response on Scheduler informing of the successfull txn speculative execution
      auto scheduler_response = ReceiveOnSchedulerWorkerSocket(0,0);

      auto [txn_id, rollback] = *scheduler_response.data<slog::TigaTxnExecutionResponse>();
      ASSERT_EQ(txn_id, txn->internal().id());
      ASSERT_FALSE(rollback);
    }
    // Reissue transaction informing of the rollback
    {
      zmq::message_t msg(sizeof(slog::TigaIssueTxnMessage));
      *msg.data<slog::TigaIssueTxnMessage>() = std::make_tuple(txn, 200, false, true);
      SchedulerSendToWorker(0, 0, std::move(msg));
    }

    // Receive response on Scheduler informing of successfull rollback
    {
      auto scheduler_response = ReceiveOnSchedulerWorkerSocket(0,0);

      auto [txn_id, rollback] = *scheduler_response.data<slog::TigaTxnExecutionResponse>();
      ASSERT_EQ(txn_id, txn->internal().id());
      ASSERT_TRUE(rollback);
    }

    // Issue a new Read transaction, it should return the previous value

    auto read_txn = slog::MakeTestTransaction(configs[0], 3000,
      {{"0", KeyType::READ, 0}},
      {{"GET", "0"}});
    read_txn->mutable_internal()->set_is_one_shot_txn(true);

    {
      zmq::message_t msg(sizeof(slog::TigaIssueTxnMessage));
      *msg.data<slog::TigaIssueTxnMessage>() = std::make_tuple(read_txn, 300, false, false);
      SchedulerSendToWorker(0, 0, std::move(msg));
    }

    {
      auto response = ReceiveOnServerChannel(0);

      auto finished_tiga_sub_txn = response->mutable_request()->mutable_tiga_finished_subtxn();
      ASSERT_EQ(finished_tiga_sub_txn->timestamp(), 300);

      auto finished_txn = finished_tiga_sub_txn->mutable_finished_subtxn()->mutable_txn();
      ASSERT_EQ(finished_txn->internal().id(), read_txn->internal().id());
      ASSERT_EQ(finished_txn->status(), slog::TransactionStatus::COMMITTED);
      ASSERT_EQ(finished_txn->keys_size(), 1);
      ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").type(), KeyType::READ);
      ASSERT_EQ(slog::TxnValueEntry(*finished_txn, "0").value(), "New0");

      // Get response on Scheduler informing of the successfull txn speculative execution
      auto scheduler_response = ReceiveOnSchedulerWorkerSocket(0,0);

      auto [txn_id, rollback] = *scheduler_response.data<slog::TigaTxnExecutionResponse>();
      ASSERT_EQ(txn_id, read_txn->internal().id());
      ASSERT_FALSE(rollback);
    }
  }

}

TEST_F(WorkerTest, Successful2FISpeculativeExecution){}

TEST_F(WorkerTest, Rollback2FITransactionBeforeReadSetExchange){}

TEST_F(WorkerTest, Rollback2FITransactionAfterReadSetExchange){}

