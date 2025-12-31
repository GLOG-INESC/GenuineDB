#include <gtest/gtest.h>

#include <vector>

#include "common/proto_utils.h"
#include "test/test_utils.h"

using namespace std;
using namespace slog;

class SchedulerTest : public ::testing::Test {
 protected:
  static const size_t kNumMachines = 6;
  static const uint32_t kNumRegions = 2;
  static const uint32_t kNumPartitions = 3;

  virtual ConfigVec MakeConfigs() {
    internal::Configuration extra_config;
    extra_config.set_bypass_mh_orderer(true);
    extra_config.set_one_shot_txns(true);

    return MakeTestConfigurations("scheduler", kNumRegions, 1, kNumPartitions, extra_config);}

  void SetUp() {

    ConfigVec configs = MakeConfigs();

    for (size_t i = 0; i < kNumMachines; i++) {
      test_slogs[i] = make_unique<TestSlog>(configs[i]);
      test_slogs[i]->AddScheduler();
      sender[i] = test_slogs[i]->NewSender();
      test_slogs[i]->AddOutputSocket(kServerChannel);
    }



    // Relica 0
    test_slogs[0]->Data("A", {"valueA", 0, 1});
    test_slogs[0]->Data("D", {"valueD", 0, 1});
    test_slogs[0]->Data("Y", {"valueY", 1, 1});

    test_slogs[1]->Data("C", {"valueC", 0, 1});
    test_slogs[1]->Data("F", {"valueF", 0, 1});
    test_slogs[1]->Data("X", {"valueX", 1, 1});
    test_slogs[1]->Data("L", {"valueL", 1, 1});

    test_slogs[2]->Data("B", {"valueB", 0, 1});
    test_slogs[2]->Data("E", {"valueE", 0, 1});
    test_slogs[2]->Data("Z", {"valueZ", 1, 1});

    // Region 1
    test_slogs[3]->Data("A", {"valueA", 0, 1});
    test_slogs[3]->Data("D", {"valueD", 0, 1});
    test_slogs[3]->Data("Y", {"valueY", 1, 1});

    test_slogs[4]->Data("C", {"valueC", 0, 1});
    test_slogs[4]->Data("F", {"valueF", 0, 1});
    test_slogs[4]->Data("X", {"valueX", 1, 1});
    test_slogs[4]->Data("L", {"valueL", 1, 1});

    test_slogs[5]->Data("B", {"valueB", 0, 1});
    test_slogs[5]->Data("E", {"valueE", 0, 1});
    test_slogs[5]->Data("Z", {"valueZ", 1, 1});

    for (const auto& test_slog : test_slogs) {
      test_slog->StartInNewThreads();
    }
  }

#ifdef PARTIAL_EXEC
  void SendTransaction(Transaction* txn, RegionId lock_only_region = std::numeric_limits<RegionId>::max()) {
    CHECK(txn != nullptr);
    auto sharder = Sharder::MakeSharder(test_slogs[0]->config());

    for (auto p : txn->internal().involved_partitions()){
      if ( lock_only_region != (RegionId)-1){
        if (GET_REGION_ID(p) != lock_only_region){
          continue;
        }
      }

      auto new_txn = GeneratePartitionedTxn(sharder, txn, GET_REGION_ID(p), GET_PARTITION_ID(p), false);
      if (new_txn != nullptr) {
        LOG(INFO) << "ISSUING TXN TO " << MACHINE_ID_STR(::MakeMachineId(GET_REGION_ID(p), 0, GET_PARTITION_ID(p)));
        internal::Envelope env;
        env.mutable_request()->mutable_forward_txn()->set_allocated_txn(new_txn);
        // This message is sent from machine 0:0, so it will be queued up in queue 0.
        // 'p' just happens to be the same as machine id here.
        sender[0]->Send(env, ::MakeMachineId(GET_REGION_ID(p), 0, GET_PARTITION_ID(p)), kSchedulerChannel);
        #ifdef FULL_REP
        for (auto r : txn->internal().involved_regions()) {
          if (r != GET_REGION_ID(p)){
            LOG(INFO) << "ISSUING TXN TO " << MACHINE_ID_STR(::MakeMachineId(r, 0, GET_PARTITION_ID(p)));

            sender[0]->Send(env, ::MakeMachineId(r, 0, GET_PARTITION_ID(p)), kSchedulerChannel);
          }
        }
        #endif
      }
    }

  }
#endif


#ifdef PARTIAL_EXEC
  Transaction ReceiveMultipleAndMerge(uint32_t receiver, uint32_t num_partitions, uint32_t involved_partitions) {
    Transaction txn;
    bool first_time = true;
    for (uint32_t i = 0; i < num_partitions; i++) {
      auto req_env = test_slogs[receiver]->ReceiveFromOutputSocket(kServerChannel);
      CHECK(req_env != nullptr);
      CHECK_EQ(req_env->request().type_case(), internal::Request::kFinishedSubtxn);
      auto finished_subtxn = req_env->request().finished_subtxn();
      auto sub_txn = finished_subtxn.txn();
      CHECK_EQ(sub_txn.internal().involved_partitions_size(), involved_partitions);

      if (first_time) {
        txn = sub_txn;
      } else {
        CHECK(txn.internal().id() == sub_txn.internal().id()) << "Transactions out of order";
        MergeTransaction(txn, sub_txn);
      }

      first_time = false;
    }
    return txn;
  }

#else

  Transaction ReceiveMultipleAndMerge(uint32_t receiver, uint32_t num_partitions, uint32_t involved_partitions) {
    Transaction txn;
    bool first_time = true;
    for (uint32_t i = 0; i < num_partitions; i++) {
      auto req_env = test_slogs[receiver]->ReceiveFromOutputSocket(kServerChannel);
      CHECK(req_env != nullptr);
      CHECK_EQ(req_env->request().type_case(), internal::Request::kFinishedSubtxn);
      auto finished_subtxn = req_env->request().finished_subtxn();
      auto sub_txn = finished_subtxn.txn();
      CHECK_EQ(sub_txn.internal().involved_partitions_size(), involved_partitions);

      if (first_time) {
        txn = sub_txn;
      } else {
        CHECK(txn.internal().id() == sub_txn.internal().id()) << "Transactions out of order";
        MergeTransaction(txn, sub_txn);
      }

      first_time = false;
    }
    return txn;
  }
#endif

  Transaction ReceiveMultipleAndMerge(uint32_t receiver, std::set<uint32_t> partitions, uint32_t involved_partitions) {
    Transaction txn;
    bool first_time = true;
    for (auto i : partitions) {
      auto req_env = test_slogs[receiver]->ReceiveFromOutputSocket(kServerChannel);
      CHECK(req_env != nullptr);
      CHECK_EQ(req_env->request().type_case(), internal::Request::kFinishedSubtxn);
      auto finished_subtxn = req_env->request().finished_subtxn();
      auto sub_txn = finished_subtxn.txn();
      CHECK_EQ(sub_txn.internal().involved_partitions_size(), involved_partitions);

      if (first_time) {
        txn = sub_txn;
      } else {
        CHECK(txn.internal().id() == sub_txn.internal().id()) << "Transactions out of order";
        MergeTransaction(txn, sub_txn);
      }

      first_time = false;
    }
    return txn;
  }

  MachineId MakeMachineId(int region, int partition) { return region * kNumPartitions + partition; }

  unique_ptr<TestSlog> test_slogs[kNumMachines];
  unique_ptr<Sender> sender[kNumMachines];
};

TEST_F(SchedulerTest, SinglePartitionTransaction) {
  auto txn = MakeTestTransaction(test_slogs[0]->config(), 1000,
                                 {{"A", KeyType::READ, {{0, 1}}}, {"D", KeyType::WRITE, {{0, 1}}}},
                                 {{"GET", "A"}, {"SET", "D", "newD"}}, {}, MakeMachineId(0, 1));

  SendTransaction(txn);

  auto output_txn = ReceiveMultipleAndMerge(1, 1, 1);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.keys_size(), 2);
  ASSERT_EQ(TxnValueEntry(output_txn, "A").type(), KeyType::READ);
  ASSERT_EQ(TxnValueEntry(output_txn, "A").value(), "valueA");
  ASSERT_EQ(TxnValueEntry(output_txn, "D").type(), KeyType::WRITE);
  ASSERT_EQ(TxnValueEntry(output_txn, "D").value(), "valueD");
  ASSERT_EQ(TxnValueEntry(output_txn, "D").new_value(), "newD");
}


#ifdef PARTIAL_EXEC
TEST_F(SchedulerTest, MultiHomeSinglePartitionTransaction) {
  auto txn = MakeTestTransaction(test_slogs[0]->config(), 1000,
                                 {{"A", KeyType::WRITE, {{0, 1}}}, {"Y", KeyType::WRITE, {{1, 1}}}},
                                 {{"SET", "A", "newA"}, {"SET", "Y", "newY"}}, {}, 0);
  auto sharder = Sharder::MakeSharder(test_slogs[0]->config());

  auto lo_txn_0 = GenerateLockOnlyTxn(txn, 0);
  auto lo_txn_1 = GenerateLockOnlyTxn(txn, 1);

  SendTransaction(lo_txn_0, 0);
  SendTransaction(lo_txn_1, 1);

  auto output_txn = ReceiveMultipleAndMerge(0, 2, 2);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.keys_size(), 2);
  ASSERT_EQ(TxnValueEntry(output_txn, "A").type(), KeyType::WRITE);
  ASSERT_EQ(TxnValueEntry(output_txn, "A").value(), "valueA");
  ASSERT_EQ(TxnValueEntry(output_txn, "A").new_value(), "newA");
  ASSERT_EQ(TxnValueEntry(output_txn, "Y").type(), KeyType::WRITE);
  ASSERT_EQ(TxnValueEntry(output_txn, "Y").value(), "valueY");
  ASSERT_EQ(TxnValueEntry(output_txn, "Y").new_value(), "newY");
}
#else
TEST_F(SchedulerTest, MultiHomeSinglePartitionTransaction) {
  auto txn = MakeTestTransaction(test_slogs[0]->config(), 1000,
                                 {{"A", KeyType::WRITE, {{0, 1}}}, {"Y", KeyType::WRITE, {{1, 1}}}},
                                 {{"SET", "A", "newA"}, {"SET", "Y", "newY"}}, {}, 0);
  auto sharder = Sharder::MakeSharder(test_slogs[0]->config());

  for (int i = 0; i < 2; i++)
  {
    auto lo_txn = GenerateLockOnlyTxn(txn, i);

    auto new_txn = GeneratePartitionedTxn(sharder, lo_txn, 0);
    if (new_txn != nullptr) {
      internal::Envelope env;
      env.mutable_request()->mutable_forward_txn()->set_allocated_txn(new_txn);
      // This message is sent from machine 0:0, so it will be queued up in queue 0.
      // 'p' just happens to be the same as machine id here.
      sender[0]->Send(env, 0, kSchedulerChannel);
    }
  }


  //SendTransaction(txn);

  auto output_txn = ReceiveMultipleAndMerge(1, 1, 1);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.keys_size(), 2);
  ASSERT_EQ(TxnValueEntry(output_txn, "A").type(), KeyType::READ);
  ASSERT_EQ(TxnValueEntry(output_txn, "A").value(), "valueA");
  ASSERT_EQ(TxnValueEntry(output_txn, "D").type(), KeyType::WRITE);
  ASSERT_EQ(TxnValueEntry(output_txn, "D").value(), "valueD");
  ASSERT_EQ(TxnValueEntry(output_txn, "D").new_value(), "newD");
}

#endif


TEST_F(SchedulerTest, MultiPartitionTransaction1Active1Passive) {
  auto txn =
      MakeTestTransaction(test_slogs[0]->config(), 1000,
                          {{"A", KeyType::READ, {{0, 1}}}, {"C", KeyType::WRITE, {{0, 1}}}}, {{"COPY", "A", "C"}});

  SendTransaction(txn);

  auto output_txn = ReceiveMultipleAndMerge(0, 2, 2);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.keys_size(), 2);
  ASSERT_EQ(TxnValueEntry(output_txn, "A").type(), KeyType::READ);
  ASSERT_EQ(TxnValueEntry(output_txn, "A").value(), "valueA");
  ASSERT_EQ(TxnValueEntry(output_txn, "C").type(), KeyType::WRITE);
  ASSERT_EQ(TxnValueEntry(output_txn, "C").value(), "valueC");
  ASSERT_EQ(TxnValueEntry(output_txn, "C").new_value(), "valueA");
}

TEST_F(SchedulerTest, MultiPartitionTransaction2Active) {
  auto txn = MakeTestTransaction(test_slogs[0]->config(), 1000,
                                   {{"A", KeyType::WRITE, {{0, 1}}}, {"B", KeyType::WRITE, {{0, 1}}}},
                                   {{"SET", "A", "test_t11"}, {"SET", "B", "test_t11"}});
  SendTransaction(txn);

  auto output_txn = ReceiveMultipleAndMerge(0, 2, 2);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.keys_size(), 2);
  ASSERT_EQ(TxnValueEntry(output_txn, "A").type(), KeyType::WRITE);
  ASSERT_EQ(TxnValueEntry(output_txn, "A").value(), "valueA");
  ASSERT_EQ(TxnValueEntry(output_txn, "A").new_value(), "test_t11");

  ASSERT_EQ(TxnValueEntry(output_txn, "B").type(), KeyType::WRITE);
  ASSERT_EQ(TxnValueEntry(output_txn, "B").value(), "valueB");
  ASSERT_EQ(TxnValueEntry(output_txn, "B").new_value(), "test_t11");
}

TEST_F(SchedulerTest, RemoteMultiPartitionTransaction2Active) {
  auto txn = MakeTestTransaction(test_slogs[0]->config(), 1000,
                                 {{"Y", KeyType::WRITE, {{1, 1}}}, {"X", KeyType::WRITE, {{1, 1}}}},
                                 {{"SET", "Y", "test_t11"}, {"SET", "X", "test_t11"}});
  SendTransaction(txn);

  auto output_txn = ReceiveMultipleAndMerge(0, 2, 2);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.keys_size(), 2);
  ASSERT_EQ(TxnValueEntry(output_txn, "Y").type(), KeyType::WRITE);
  ASSERT_EQ(TxnValueEntry(output_txn, "Y").value(), "valueY");
  ASSERT_EQ(TxnValueEntry(output_txn, "Y").new_value(), "test_t11");

  ASSERT_EQ(TxnValueEntry(output_txn, "X").type(), KeyType::WRITE);
  ASSERT_EQ(TxnValueEntry(output_txn, "X").value(), "valueX");
  ASSERT_EQ(TxnValueEntry(output_txn, "X").new_value(), "test_t11");
}

TEST_F(SchedulerTest, MultiPartitionTransactionMutualWait2Partitions) {
  auto txn = MakeTestTransaction(test_slogs[0]->config(), 1000,
                                 {{"B", KeyType::WRITE, {{0, 1}}}, {"C", KeyType::WRITE, {{0, 1}}}},
                                 {{"COPY", "C", "B"}, {"COPY", "B", "C"}});

  SendTransaction(txn);

  auto output_txn = ReceiveMultipleAndMerge(0, 2, 2);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.keys_size(), 2);
  ASSERT_EQ(TxnValueEntry(output_txn, "B").type(), KeyType::WRITE);
  ASSERT_EQ(TxnValueEntry(output_txn, "B").value(), "valueB");
  ASSERT_EQ(TxnValueEntry(output_txn, "B").new_value(), "valueC");
  ASSERT_EQ(TxnValueEntry(output_txn, "C").type(), KeyType::WRITE);
  ASSERT_EQ(TxnValueEntry(output_txn, "C").value(), "valueC");
  ASSERT_EQ(TxnValueEntry(output_txn, "C").new_value(), "valueB");
}

TEST_F(SchedulerTest, MultiPartitionTransactionWriteOnly) {
  auto txn = MakeTestTransaction(
      test_slogs[0]->config(), 1000,
      {{"A", KeyType::WRITE, {{0, 1}}}, {"B", KeyType::WRITE, {{0, 1}}}, {"C", KeyType::WRITE, {{0, 1}}}},
      {{"SET", "A", "newA"}, {"SET", "B", "newB"}, {"SET", "C", "newC"}});

  SendTransaction(txn);

  auto output_txn = ReceiveMultipleAndMerge(0, 3, 3);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.keys_size(), 3);
  ASSERT_EQ(TxnValueEntry(output_txn, "A").type(), KeyType::WRITE);
  ASSERT_EQ(TxnValueEntry(output_txn, "A").value(), "valueA");
  ASSERT_EQ(TxnValueEntry(output_txn, "A").new_value(), "newA");
  ASSERT_EQ(TxnValueEntry(output_txn, "B").type(), KeyType::WRITE);
  ASSERT_EQ(TxnValueEntry(output_txn, "B").value(), "valueB");
  ASSERT_EQ(TxnValueEntry(output_txn, "B").new_value(), "newB");
  ASSERT_EQ(TxnValueEntry(output_txn, "C").type(), KeyType::WRITE);
  ASSERT_EQ(TxnValueEntry(output_txn, "C").value(), "valueC");
  ASSERT_EQ(TxnValueEntry(output_txn, "C").new_value(), "newC");
}

TEST_F(SchedulerTest, MultiHomeSinglePartitionTransactionWriteOnly) {
  auto txn = MakeTestTransaction(
      test_slogs[0]->config(), 1000,
      {{"A", KeyType::WRITE, {{0, 1}}}, {"Y", KeyType::WRITE, {{1, 1}}}},
      {{"SET", "A", "newA"}, {"SET", "Y", "newY"}});

  auto lo_txn_0 = GenerateLockOnlyTxn(txn, 0);
  auto lo_txn_1 = GenerateLockOnlyTxn(txn, 1);
  SendTransaction(lo_txn_0, 0);
  SendTransaction(lo_txn_1, 1);

  auto output_txn = ReceiveMultipleAndMerge(0, 2, 2);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.keys_size(), 2);
  ASSERT_EQ(TxnValueEntry(output_txn, "A").type(), KeyType::WRITE);
  ASSERT_EQ(TxnValueEntry(output_txn, "A").value(), "valueA");
  ASSERT_EQ(TxnValueEntry(output_txn, "A").new_value(), "newA");
  ASSERT_EQ(TxnValueEntry(output_txn, "Y").type(), KeyType::WRITE);
  ASSERT_EQ(TxnValueEntry(output_txn, "Y").value(), "valueY");
  ASSERT_EQ(TxnValueEntry(output_txn, "Y").new_value(), "newY");
}

TEST_F(SchedulerTest, MultiPartitionTransactionReadOnly) {
  auto txn = MakeTestTransaction(
      test_slogs[0]->config(), 1000,
      {{"D", KeyType::READ, {{0, 1}}}, {"E", KeyType::READ, {{0, 1}}}, {"F", KeyType::READ, {{0, 1}}}},
      {{"GET", "D"}, {"GET", "E"}, {"GET", "F"}});

  SendTransaction(txn);

  auto output_txn = ReceiveMultipleAndMerge(0, 3, 3);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.keys_size(), 3);
  ASSERT_EQ(TxnValueEntry(output_txn, "D").type(), KeyType::READ);
  ASSERT_EQ(TxnValueEntry(output_txn, "D").value(), "valueD");
  ASSERT_EQ(TxnValueEntry(output_txn, "E").type(), KeyType::READ);
  ASSERT_EQ(TxnValueEntry(output_txn, "E").value(), "valueE");
  ASSERT_EQ(TxnValueEntry(output_txn, "F").type(), KeyType::READ);
  ASSERT_EQ(TxnValueEntry(output_txn, "F").value(), "valueF");
}

TEST_F(SchedulerTest, SimpleMultiHomeBatch) {
  auto txn = MakeTestTransaction(
      test_slogs[0]->config(), 1000,
      {{"A", KeyType::READ, {{0, 1}}},
       {"X", KeyType::READ, {{1, 1}}},
       {"C", KeyType::READ, {{0, 1}}},
       {"B", KeyType::WRITE, {{0, 1}}},
       {"Y", KeyType::WRITE, {{1, 1}}},
       {"Z", KeyType::WRITE, {{1, 1}}}},
      {{"GET", "A"}, {"GET", "X"}, {"GET", "C"}, {"SET", "B", "newB"}, {"SET", "Y", "newY"}, {"SET", "Z", "newZ"}});

  auto lo_txn_0 = GenerateLockOnlyTxn(txn, 0);
  auto lo_txn_1 = GenerateLockOnlyTxn(txn, 1);

  delete txn;

  SendTransaction(lo_txn_0, 0);
  SendTransaction(lo_txn_1, 1);

  auto output_txn = ReceiveMultipleAndMerge(0, 3, 6);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.keys_size(), 6);
  ASSERT_EQ(TxnValueEntry(output_txn, "A").type(), KeyType::READ);
  ASSERT_EQ(TxnValueEntry(output_txn, "A").value(), "valueA");
  ASSERT_EQ(TxnValueEntry(output_txn, "X").type(), KeyType::READ);
  ASSERT_EQ(TxnValueEntry(output_txn, "X").value(), "valueX");
  ASSERT_EQ(TxnValueEntry(output_txn, "C").type(), KeyType::READ);
  ASSERT_EQ(TxnValueEntry(output_txn, "C").value(), "valueC");
  ASSERT_EQ(TxnValueEntry(output_txn, "B").type(), KeyType::WRITE);
  ASSERT_EQ(TxnValueEntry(output_txn, "B").value(), "valueB");
  ASSERT_EQ(TxnValueEntry(output_txn, "B").new_value(), "newB");
  ASSERT_EQ(TxnValueEntry(output_txn, "Y").type(), KeyType::WRITE);
  ASSERT_EQ(TxnValueEntry(output_txn, "Y").value(), "valueY");
  ASSERT_EQ(TxnValueEntry(output_txn, "Y").new_value(), "newY");
  ASSERT_EQ(TxnValueEntry(output_txn, "Z").type(), KeyType::WRITE);
  ASSERT_EQ(TxnValueEntry(output_txn, "Z").value(), "valueZ");
  ASSERT_EQ(TxnValueEntry(output_txn, "Z").new_value(), "newZ");
}

TEST_F(SchedulerTest, SinglePartitionTransactionValidateMasters) {
  auto txn = MakeTestTransaction(test_slogs[0]->config(), 1000,
                                 {{"A", KeyType::READ, {{0, 1}}}, {"D", KeyType::WRITE, {{0, 1}}}},
                                 {{"GET", "A"}, {"SET", "D", "newD"}}, {}, MakeMachineId(0, 1));

  SendTransaction(txn);

  auto output_txn = ReceiveMultipleAndMerge(1, 1, 1);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.keys_size(), 2);
  ASSERT_EQ(TxnValueEntry(output_txn, "A").type(), KeyType::READ);
  ASSERT_EQ(TxnValueEntry(output_txn, "A").value(), "valueA");
  ASSERT_EQ(TxnValueEntry(output_txn, "D").type(), KeyType::WRITE);
  ASSERT_EQ(TxnValueEntry(output_txn, "D").value(), "valueD");
  ASSERT_EQ(TxnValueEntry(output_txn, "D").new_value(), "newD");
}

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
TEST_F(SchedulerTest, SinglePartitionTransactionProcessRemaster) {
  auto remaster_txn = MakeTestTransaction(test_slogs[0]->config(), 2000, {{"A", KeyType::WRITE, {{0, 1}}}}, {},
                                          1 /* remaster */, MakeMachineId(0, 1));
  auto txn = MakeTestTransaction(test_slogs[0]->config(), 1000, {{"A", KeyType::READ, {{1, 2}}}}, {{"GET", "A"}}, {},
                                 MakeMachineId(0, 0));

  SendTransaction(txn);
  SendTransaction(remaster_txn);

  auto output_remaster_txn = ReceiveMultipleAndMerge(1, 1);
  LOG(INFO) << output_remaster_txn;
  ASSERT_EQ(output_remaster_txn.internal().id(), 2000);
  ASSERT_EQ(output_remaster_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_remaster_txn.remaster().new_master(), 1);

  auto output_txn = ReceiveMultipleAndMerge(0, 1);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.internal().id(), 1000);
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.keys_size(), 1);
  ASSERT_EQ(TxnValueEntry(output_txn, "A").type(), KeyType::READ);
  ASSERT_EQ(TxnValueEntry(output_txn, "A").value(), "valueA");
}
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY) */

#ifdef REMASTER_PROTOCOL_COUNTERLESS
TEST_F(SchedulerTest, SinglePartitionTransactionProcessRemaster) {
  GTEST_SKIP() << "Test not implemented yet.";
  auto remaster_txn =
      MakeTestTransaction(test_slogs[0]->config(), 1000, {{"A", KeyType::WRITE, 0}}, {}, 1, /* new master */
                          MakeMachineId(0, 1));

  auto remaster_txn_lo_0 = GenerateLockOnlyTxn(remaster_txn, 0);
  auto remaster_txn_lo_1 = GenerateLockOnlyTxn(remaster_txn, 1);

  delete remaster_txn;

  auto txn = MakeTestTransaction(test_slogs[0]->config(), 2000, {{"A", KeyType::READ, 1}}, {{"GET A"}}, {},
                                 MakeMachineId(0, 0));

  SendTransaction(remaster_txn_lo_1);
  SendTransaction(remaster_txn_lo_0);
  SendTransaction(txn);

  auto output_remaster_txn = ReceiveMultipleAndMerge(1, 2, 2);
  LOG(INFO) << output_remaster_txn;
  ASSERT_EQ(output_remaster_txn.internal().id(), 1000U);
  ASSERT_EQ(output_remaster_txn.status(), TransactionStatus::COMMITTED);

  auto output_txn = ReceiveMultipleAndMerge(0, 1, 1);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.internal().id(), 2000U);
  ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(output_txn.keys_size(), 1);
  ASSERT_EQ(TxnValueEntry(output_txn, "A").type(), KeyType::READ);
  ASSERT_EQ(TxnValueEntry(output_txn, "A").value(), "valueA");
}
#endif /* REMASTERING_PROTOCOL_COUNTERLESS */

TEST_F(SchedulerTest, AbortSingleHomeSinglePartition) {
  auto txn = MakeTestTransaction(test_slogs[0]->config(), 1000, {{"A", KeyType::READ, {{1, 0}}}}, {{"GET", "A"}}, {},
                                 MakeMachineId(0, 1));

  SendTransaction(txn);

  auto output_txn = ReceiveMultipleAndMerge(1, 1, 1);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::ABORTED);
}

TEST_F(SchedulerTest, AbortMultiHomeSinglePartition) {
  auto txn = MakeTestTransaction(
      test_slogs[0]->config(), 1000,
      {{"A", KeyType::READ, {{0, 1}}}, {"D", KeyType::READ, {{1, 0}}}, {"Y", KeyType::WRITE, {{1, 1}}}});

  auto lo_txn_0 = GenerateLockOnlyTxn(txn, 0);
  auto lo_txn_1 = GenerateLockOnlyTxn(txn, 1);

  delete txn;

  SendTransaction(lo_txn_0, 0);
  SendTransaction(lo_txn_1, 1);

  auto output_txn = ReceiveMultipleAndMerge(0, 2, 2);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::ABORTED);
}

TEST_F(SchedulerTest, AbortSingleHomeMultiPartition) {
  auto txn = MakeTestTransaction(test_slogs[0]->config(), 1000,
                                 {{"A", KeyType::READ, {{1, 0}}}, {"X", KeyType::WRITE, {{1, 1}}}},
                                 {{"GET", "A"}, {"SET", "X", "newC"}}, {}, MakeMachineId(0, 1));

  SendTransaction(txn);

  auto output_txn = ReceiveMultipleAndMerge(1, 2, 2);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::ABORTED);
}

TEST_F(SchedulerTest, AbortSingleHomeMultiPartition2Active) {
  auto txn = MakeTestTransaction(
      test_slogs[0]->config(), 1000,
      {{"Y", KeyType::READ, {{1, 1}}}, {"C", KeyType::WRITE, {{1, 0}}}, {"B", KeyType::WRITE, {{1, 0}}}},
      {{"GET", "Y"}, {"SET", "C", "newC"}, {"SET", "B", "newB"}}, {}, MakeMachineId(0, 1));

  SendTransaction(txn);

  auto output_txn = ReceiveMultipleAndMerge(1, 3, 3);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::ABORTED);
}

TEST_F(SchedulerTest, AbortMultiHomeMultiPartition2Active) {
  // D and X have outdated master information
  auto txn = MakeTestTransaction(test_slogs[0]->config(), 1000,
                                 {{"A", KeyType::READ, {{0, 1}}},
                                  {"C", KeyType::READ, {{1, 0}}},
                                  {"Y", KeyType::WRITE, {{1, 1}}},
                                  {"X", KeyType::WRITE, {{0, 0}}}});

  auto lo_txn_0 = GenerateLockOnlyTxn(txn, 0);
  auto lo_txn_1 = GenerateLockOnlyTxn(txn, 1);

  delete txn;

  SendTransaction(lo_txn_0, 0);
  SendTransaction(lo_txn_1, 1);

  auto output_txn = ReceiveMultipleAndMerge(0, 2, 4);
  LOG(INFO) << output_txn;
  ASSERT_EQ(output_txn.status(), TransactionStatus::ABORTED);
}

#ifdef LOCK_MANAGER_DDR
class SchedulerTestWithDeadlockResolver : public SchedulerTest {
 protected:
  static const size_t kNumMachines = 6;
  static const uint32_t kNumRegions = 2;
  static const uint32_t kNumPartitions = 3;

  ConfigVec MakeConfigs() final {
    internal::Configuration add_on;
    add_on.set_ddr_interval(10);
    add_on.set_bypass_mh_orderer(true);
    add_on.set_one_shot_txns(true);

    return MakeTestConfigurations("scheduler", kNumRegions, 1, kNumPartitions, add_on);
  }
};


TEST_F(SchedulerTestWithDeadlockResolver, SinglePartitionPartitionedDeadlock) {

  auto txn1 = MakeTestTransaction(test_slogs[0]->config(), 1000,
                                  {{"A", KeyType::READ, {{0, 1}}}, {"Y", KeyType::WRITE, {{1, 1}}}},
                                  {{"GET", "A"}, {"SET", "Y", "test"}}, {}, 0);

  auto lo_txn_1_0 = GenerateLockOnlyTxn(txn1, 0);
  auto lo_txn_1_1 = GenerateLockOnlyTxn(txn1, 1);

  delete txn1;

  auto txn2 = MakeTestTransaction(test_slogs[0]->config(), 2000,
                                  {{"A", KeyType::WRITE, {{0, 1}}}, {"Y", KeyType::READ, {{1, 1}}}},
                                  {{"COPY", "Y", "A"}}, {}, 1);
  auto lo_txn_2_0 = GenerateLockOnlyTxn(txn2, 0);
  auto lo_txn_2_1 = GenerateLockOnlyTxn(txn2, 1);
  delete txn2;

  // This would cause a partitioned deadlock. After the deadlock is resolved, the
  // txns must be run in the order txn1 then txn2
  SendTransaction(lo_txn_1_0, 0);
  SendTransaction(lo_txn_2_0, 0);
  SendTransaction(lo_txn_2_1, 1);
  SendTransaction(lo_txn_1_1, 1);

  {
    auto output_txn = ReceiveMultipleAndMerge(0, 2, 2);
    LOG(INFO) << output_txn;
    ASSERT_EQ(output_txn.internal().id(), 1000);
    ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(output_txn.keys_size(), 2);
    ASSERT_EQ(TxnValueEntry(output_txn, "A").type(), KeyType::READ);
    ASSERT_EQ(TxnValueEntry(output_txn, "A").value(), "valueA");
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").value(), "valueY");
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").new_value(), "test");
  }

  {
    auto output_txn = ReceiveMultipleAndMerge(1, 2, 2);
    LOG(INFO) << output_txn;
    ASSERT_EQ(output_txn.internal().id(), 2000);
    ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(output_txn.keys_size(), 2);
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").type(), KeyType::READ);
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").value(), "test");
    ASSERT_EQ(TxnValueEntry(output_txn, "A").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "A").value(), "valueA");
    ASSERT_EQ(TxnValueEntry(output_txn, "A").new_value(), "test");
  }
}

TEST_F(SchedulerTestWithDeadlockResolver, MultiPartitionPartitionedDeadlock) {
  auto txn1 = MakeTestTransaction(test_slogs[0]->config(), 1000,
                                  {{"A", KeyType::WRITE, {{0, 1}}}, {"Y", KeyType::WRITE, {{1, 1}}},
                                   {"C", KeyType::WRITE, {{0, 1}}}, {"X", KeyType::WRITE, {{1, 1}}}},
                                  {{"SET", "A", "test_t1"}, {"SET", "Y", "test_t1"}, {"SET", "C", "test_t1"}, {"SET", "X", "test_t1"}}, {}, 0);
  auto lo_txn_1_0 = GenerateLockOnlyTxn(txn1, 0);
  auto lo_txn_1_1 = GenerateLockOnlyTxn(txn1, 1);
  delete txn1;

  auto txn2 = MakeTestTransaction(test_slogs[0]->config(), 2000,
                                  {{"A", KeyType::WRITE, {{0, 1}}}, {"Y", KeyType::WRITE, {{1, 1}}},
                                   {"C", KeyType::WRITE, {{0, 1}}}, {"X", KeyType::WRITE, {{1, 1}}}},
                                  {{"SET", "A", "test_t2"}, {"SET", "Y", "test_t2"}, {"SET", "C", "test_t2"}, {"SET", "X", "test_t2"}}, {}, 1);
  auto lo_txn_2_0 = GenerateLockOnlyTxn(txn2, 0);
  auto lo_txn_2_1 = GenerateLockOnlyTxn(txn2, 1);
  delete txn2;

  // This would cause a partitioned deadlock. After the deadlock is resolved, the
  // txns must be run in the order txn1 then txn2
  SendTransaction(lo_txn_1_0, 0);
  SendTransaction(lo_txn_2_0, 0);
  SendTransaction(lo_txn_2_1, 1);
  SendTransaction(lo_txn_1_1, 1);

  {
    auto output_txn = ReceiveMultipleAndMerge(0, 2, 4);
    LOG(INFO) << output_txn;
    ASSERT_EQ(output_txn.internal().id(), 1000);
    ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(output_txn.keys_size(), 4);
    ASSERT_EQ(TxnValueEntry(output_txn, "A").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "A").value(), "valueA");
    ASSERT_EQ(TxnValueEntry(output_txn, "A").new_value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").value(), "valueY");
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").new_value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "C").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "C").value(), "valueC");
    ASSERT_EQ(TxnValueEntry(output_txn, "C").new_value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "X").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "X").value(), "valueX");
    ASSERT_EQ(TxnValueEntry(output_txn, "X").new_value(), "test_t1");
  }

  {
    auto output_txn = ReceiveMultipleAndMerge(1, 2, 4);
    LOG(INFO) << output_txn;
    ASSERT_EQ(output_txn.internal().id(), 2000);
    ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(output_txn.keys_size(), 4);
    ASSERT_EQ(TxnValueEntry(output_txn, "A").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "A").value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "A").new_value(), "test_t2");
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").new_value(), "test_t2");
    ASSERT_EQ(TxnValueEntry(output_txn, "C").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "C").value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "C").new_value(), "test_t2");
    ASSERT_EQ(TxnValueEntry(output_txn, "X").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "X").value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "X").new_value(), "test_t2");

  }
}

TEST_F(SchedulerTestWithDeadlockResolver, MultiAndSinglePartitionedDeadlock) {
  auto txn1 = MakeTestTransaction(test_slogs[0]->config(), 1000,
                                  {{"A", KeyType::WRITE, {{0, 1}}}, {"Y", KeyType::WRITE, {{1, 1}}},
                                   {"C", KeyType::WRITE, {{0, 1}}}, {"X", KeyType::WRITE, {{1, 1}}}},
                                  {{"SET", "A", "test_t1"}, {"SET", "Y", "test_t1"}, {"SET", "C", "test_t1"}, {"SET", "X", "test_t1"}}, {}, 0);

  auto lo_txn_1_0 = GenerateLockOnlyTxn(txn1, 0);
  auto lo_txn_1_1 = GenerateLockOnlyTxn(txn1, 1);
  delete txn1;

  auto txn11 = MakeTestTransaction(test_slogs[0]->config(), 1500,
                                  {{"A", KeyType::WRITE, {{0, 1}}}, {"C", KeyType::WRITE, {{0, 1}}}},
                                  {{"SET", "A", "test_t11"}, {"SET", "C", "test_t11"}}, {}, 0);

  auto txn2 = MakeTestTransaction(test_slogs[0]->config(), 2000,
                                  {{"A", KeyType::WRITE, {{0, 1}}}, {"Y", KeyType::WRITE, {{1, 1}}},
                                   {"C", KeyType::WRITE, {{0, 1}}}, {"X", KeyType::WRITE, {{1, 1}}}},
                                  {{"SET", "A", "test_t2"}, {"SET", "Y", "test_t2"}, {"SET", "C", "test_t2"}, {"SET", "X", "test_t2"}}, {}, 1);
  auto lo_txn_2_0 = GenerateLockOnlyTxn(txn2, 0);
  auto lo_txn_2_1 = GenerateLockOnlyTxn(txn2, 1);
  delete txn2;

  // This would cause a partitioned deadlock. After the deadlock is resolved, the
  // txns must be run in the order txn1 then txn2
  SendTransaction(lo_txn_1_0, 0);
  sleep(2);
  SendTransaction(txn11);
  sleep(2);
  SendTransaction(lo_txn_2_0, 0);
  SendTransaction(lo_txn_2_1, 1);
  SendTransaction(lo_txn_1_1, 1);

  {
    auto output_txn = ReceiveMultipleAndMerge(0, 4, 4);
    LOG(INFO) << output_txn;
    ASSERT_EQ(output_txn.internal().id(), 1000);
    ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(output_txn.keys_size(), 4);
    ASSERT_EQ(TxnValueEntry(output_txn, "A").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "A").value(), "valueA");
    ASSERT_EQ(TxnValueEntry(output_txn, "A").new_value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").value(), "valueY");
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").new_value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "C").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "C").value(), "valueC");
    ASSERT_EQ(TxnValueEntry(output_txn, "C").new_value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "X").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "X").value(), "valueX");
    ASSERT_EQ(TxnValueEntry(output_txn, "X").new_value(), "test_t1");
  }

  {
    auto output_txn = ReceiveMultipleAndMerge(0, 2, 2);
    LOG(INFO) << output_txn;
    ASSERT_EQ(output_txn.internal().id(), 1500);
    ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(output_txn.keys_size(), 2);
    ASSERT_EQ(TxnValueEntry(output_txn, "A").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "A").value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "A").new_value(), "test_t11");
    ASSERT_EQ(TxnValueEntry(output_txn, "C").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "C").value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "C").new_value(), "test_t11");
  }

  {
    auto output_txn = ReceiveMultipleAndMerge(1, 4, 4);
    LOG(INFO) << output_txn;
    ASSERT_EQ(output_txn.internal().id(), 2000);
    ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(output_txn.keys_size(), 4);
    ASSERT_EQ(TxnValueEntry(output_txn, "A").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "A").value(), "test_t11");
    ASSERT_EQ(TxnValueEntry(output_txn, "A").new_value(), "test_t2");
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").new_value(), "test_t2");
    ASSERT_EQ(TxnValueEntry(output_txn, "C").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "C").value(), "test_t11");
    ASSERT_EQ(TxnValueEntry(output_txn, "C").new_value(), "test_t2");
    ASSERT_EQ(TxnValueEntry(output_txn, "X").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "X").value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "X").new_value(), "test_t2");

  }
}


TEST_F(SchedulerTestWithDeadlockResolver, UnrelatedPartitionPartitionedDeadlock) {
  auto txn1 = MakeTestTransaction(test_slogs[0]->config(), 1000,
                                  {{"A", KeyType::WRITE, {{0, 1}}}, {"Y", KeyType::WRITE, {{1, 1}}},
                                   {"C", KeyType::WRITE, {{0, 1}}}, {"X", KeyType::WRITE, {{1, 1}}}},
                                  {{"SET", "A", "test_t1"}, {"SET", "Y", "test_t1"}, {"SET", "C", "test_t1"}, {"SET", "X", "test_t1"}}, {}, 0);

  auto lo_txn_1_0 = GenerateLockOnlyTxn(txn1, 0);
  auto lo_txn_1_1 = GenerateLockOnlyTxn(txn1, 1);
  delete txn1;

  auto txn11 = MakeTestTransaction(test_slogs[0]->config(), 1500,
                                   {{"A", KeyType::WRITE, {{0, 1}}}, {"B", KeyType::WRITE, {{0, 1}}}},
                                   {{"SET", "A", "test_t11"}, {"SET", "B", "test_t11"}}, {}, 0);

  auto txn2 = MakeTestTransaction(test_slogs[0]->config(), 2000,
                                  {{"A", KeyType::WRITE, {{0, 1}}}, {"Y", KeyType::WRITE, {{1, 1}}},
                                   {"C", KeyType::WRITE, {{0, 1}}}, {"X", KeyType::WRITE, {{1, 1}}}},
                                  {{"SET", "A", "test_t2"}, {"SET", "Y", "test_t2"}, {"SET", "C", "test_t2"}, {"SET", "X", "test_t2"}}, {}, 1);
  auto lo_txn_2_0 = GenerateLockOnlyTxn(txn2, 0);
  auto lo_txn_2_1 = GenerateLockOnlyTxn(txn2, 1);
  delete txn2;

  // This would cause a partitioned deadlock. After the deadlock is resolved, the
  // txns must be run in the order txn1 then txn2
  SendTransaction(lo_txn_1_0, 0);
  SendTransaction(txn11);
  SendTransaction(lo_txn_2_0, 0);
  SendTransaction(lo_txn_2_1, 1);
  SendTransaction(lo_txn_1_1, 1);

  {
    auto output_txn = ReceiveMultipleAndMerge(0, 4, 4);

    LOG(INFO) << output_txn;
    ASSERT_EQ(output_txn.internal().id(), 1000);
    ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(output_txn.keys_size(), 4);
    ASSERT_EQ(TxnValueEntry(output_txn, "A").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "A").value(), "valueA");
    ASSERT_EQ(TxnValueEntry(output_txn, "A").new_value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").value(), "valueY");
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").new_value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "C").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "C").value(), "valueC");
    ASSERT_EQ(TxnValueEntry(output_txn, "C").new_value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "X").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "X").value(), "valueX");
    ASSERT_EQ(TxnValueEntry(output_txn, "X").new_value(), "test_t1");
  }

  {
    auto output_txn = ReceiveMultipleAndMerge(0, 2, 2);
    LOG(INFO) << output_txn;
    ASSERT_EQ(output_txn.internal().id(), 1500);
    ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(output_txn.keys_size(), 2);
    ASSERT_EQ(TxnValueEntry(output_txn, "A").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "A").value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "A").new_value(), "test_t11");
    ASSERT_EQ(TxnValueEntry(output_txn, "B").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "B").value(), "valueB");
    ASSERT_EQ(TxnValueEntry(output_txn, "B").new_value(), "test_t11");
  }

  {
    auto output_txn = ReceiveMultipleAndMerge(1, 4, 4);
    LOG(INFO) << output_txn;
    ASSERT_EQ(output_txn.internal().id(), 2000);
    ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(output_txn.keys_size(), 4);
    ASSERT_EQ(TxnValueEntry(output_txn, "A").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "A").value(), "test_t11");
    ASSERT_EQ(TxnValueEntry(output_txn, "A").new_value(), "test_t2");
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").new_value(), "test_t2");
    ASSERT_EQ(TxnValueEntry(output_txn, "C").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "C").value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "C").new_value(), "test_t2");
    ASSERT_EQ(TxnValueEntry(output_txn, "X").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "X").value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "X").new_value(), "test_t2");

  }
}

TEST_F(SchedulerTestWithDeadlockResolver, MultiHomeMultiPartitionDeadlockRestart) {
  auto txn1 = MakeTestTransaction(test_slogs[0]->config(), 1000,
                                  {{"A", KeyType::WRITE, {{0, 1}}}, {"Y", KeyType::WRITE, {{1, 1}}},
                                   {"C", KeyType::WRITE, {{0, 1}}}, {"X", KeyType::WRITE, {{1, 1}}}},
                                  {{"SET", "A", "test_t1"}, {"SET", "Y", "test_t1"}, {"SET", "C", "test_t1"}, {"SET", "X", "test_t1"}}, {}, 0);

  auto lo_txn_1_0 = GenerateLockOnlyTxn(txn1, 0);
  auto lo_txn_1_1 = GenerateLockOnlyTxn(txn1, 1);
  delete txn1;

  auto txn11 = MakeTestTransaction(test_slogs[0]->config(), 3000,
                                   {{"C", KeyType::WRITE, {{0, 1}}}, {"X", KeyType::WRITE, {{1, 1}}},
                                    {"F", KeyType::WRITE, {{0, 1}}}, {"L", KeyType::WRITE, {{1, 1}}}},
                                   {{"SET", "C", "test_t11"}, {"SET", "X", "test_t11"}, {"SET", "F", "test_t11"}, {"SET", "L", "test_t11"}}, {}, 0);
  auto lo_txn_11_0 = GenerateLockOnlyTxn(txn11, 0);
  auto lo_txn_11_1 = GenerateLockOnlyTxn(txn11, 1);
  delete txn11;

  auto txn2 = MakeTestTransaction(test_slogs[0]->config(), 2000,
                                  {{"A", KeyType::WRITE, {{0, 1}}}, {"Y", KeyType::WRITE, {{1, 1}}},
                                   {"F", KeyType::WRITE, {{0, 1}}}, {"L", KeyType::WRITE, {{1, 1}}}},
                                  {{"SET", "A", "test_t2"}, {"SET", "Y", "test_t2"}, {"SET", "F", "test_t2"}, {"SET", "L", "test_t2"}}, {}, 1);
  auto lo_txn_2_0 = GenerateLockOnlyTxn(txn2, 0);
  auto lo_txn_2_1 = GenerateLockOnlyTxn(txn2, 1);
  delete txn2;



  // The Transaction is blocked at Partition 0 for conflict, but Partition 1 does not see a deadlock because txns touch on different regions
  // T11 is ordered consistently, but belongs to the deadlock and will be ordered after everyone
  SendTransaction(lo_txn_2_0, 0);
  SendTransaction(lo_txn_1_0, 0);
  SendTransaction(lo_txn_11_0, 0);
  SendTransaction(lo_txn_11_1, 1);
  SendTransaction(lo_txn_1_1, 1);
  SendTransaction(lo_txn_2_1, 1);


  {
    auto output_txn = ReceiveMultipleAndMerge(0, 4, 4);
    LOG(INFO) << output_txn;
    ASSERT_EQ(output_txn.internal().id(), 1000);
    ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(output_txn.keys_size(), 4);
    ASSERT_EQ(TxnValueEntry(output_txn, "A").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "A").value(), "valueA");
    ASSERT_EQ(TxnValueEntry(output_txn, "A").new_value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").value(), "valueY");
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").new_value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "C").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "C").value(), "valueC");
    ASSERT_EQ(TxnValueEntry(output_txn, "C").new_value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "X").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "X").value(), "valueX");
    ASSERT_EQ(TxnValueEntry(output_txn, "X").new_value(), "test_t1");
  }

  {
    auto output_txn = ReceiveMultipleAndMerge(1, 4, 4);
    LOG(INFO) << output_txn;
    ASSERT_EQ(output_txn.internal().id(), 2000);
    ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(output_txn.keys_size(), 4);
    ASSERT_EQ(TxnValueEntry(output_txn, "A").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "A").value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "A").new_value(), "test_t2");
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "Y").new_value(), "test_t2");
    ASSERT_EQ(TxnValueEntry(output_txn, "F").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "F").value(), "valueF");
    ASSERT_EQ(TxnValueEntry(output_txn, "F").new_value(), "test_t2");
    ASSERT_EQ(TxnValueEntry(output_txn, "L").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "L").value(), "valueL");
    ASSERT_EQ(TxnValueEntry(output_txn, "L").new_value(), "test_t2");
  }

  {
    auto output_txn = ReceiveMultipleAndMerge(0, 2, 2);
    LOG(INFO) << output_txn;
    ASSERT_EQ(output_txn.internal().id(), 3000);
    ASSERT_EQ(output_txn.status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(output_txn.keys_size(), 4);
    ASSERT_EQ(TxnValueEntry(output_txn, "C").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "C").value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "C").new_value(), "test_t11");
    ASSERT_EQ(TxnValueEntry(output_txn, "X").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "X").value(), "test_t1");
    ASSERT_EQ(TxnValueEntry(output_txn, "X").new_value(), "test_t11");
    ASSERT_EQ(TxnValueEntry(output_txn, "F").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "F").value(), "test_t2");
    ASSERT_EQ(TxnValueEntry(output_txn, "F").new_value(), "test_t11");
    ASSERT_EQ(TxnValueEntry(output_txn, "L").type(), KeyType::WRITE);
    ASSERT_EQ(TxnValueEntry(output_txn, "L").value(), "test_t2");
    ASSERT_EQ(TxnValueEntry(output_txn, "L").new_value(), "test_t11");
  }


}

#endif

int main(int argc, char* argv[]) {
  //FLAGS_v = 4;

  ::testing::InitGoogleTest(&argc, argv);
  google::InstallFailureSignalHandler();
  return RUN_ALL_TESTS();
}