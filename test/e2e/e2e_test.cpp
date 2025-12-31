#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/proto_utils.h"
#include "module/server.h"
#include "test/test_utils.h"
#include "test/general_test_utils.h"
#include "workload/basic.h"
#include "workload/tpcc.h"
#include "workload/ycsb.h"

#include "execution/tpcc/metadata_initializer.h"


using namespace std;
using namespace slog;

const int kNumRegions = 2;
const int kNumReplicas = 1;
const int kNumPartitions = 2;
const int kNumMachines = kNumRegions * kNumReplicas * kNumPartitions;
const int kDataItems = 2;

class E2ETest : public ::testing::TestWithParam<tuple<bool, int, int, int, bool>> {
 protected:
  virtual internal::Configuration CustomConfig() { return internal::Configuration(); }

  void SetUp() {
    auto param = GetParam();
    bool local_sync_rep = std::get<0>(param);

    auto custom_config = CustomConfig();
    bool is_detock = true;
    custom_config.set_num_workers(4);
    custom_config.set_num_log_managers(2);
    custom_config.set_ddr_interval(40);

    custom_config.set_forwarder_batch_duration(0);
    custom_config.set_bypass_mh_orderer(is_detock);
    custom_config.set_fs_latency_interval(200);
    custom_config.set_avg_latency_window_size(10);
    custom_config.set_timestamp_buffer_us(0);

    custom_config.set_synchronized_batching(is_detock);
    custom_config.set_sequencer_batch_duration(0);
    custom_config.set_sequencer_batch_size(100);
    custom_config.set_sequencer_rrr(true);

    custom_config.set_one_shot_txns(std::get<4>(param));

    custom_config.mutable_simple_partitioning()->set_num_records(2400000);
    custom_config.mutable_simple_partitioning()->set_record_size_bytes(100);
    auto configs =
        MakeTestConfigurations("e2e", kNumRegions, kNumReplicas, kNumPartitions, custom_config, local_sync_rep);

    //std::pair<Key, Record> data[kNumPartitions][kDataItems] = {{{"A", {"valA", 0, 0}}, {"C", {"valC", 1, 0}}},{{"B", {"valB", 0, 0}}, {"X", {"valX", 1, 0}}}};

    std::pair<Key, Record> data[kNumPartitions][kDataItems] = {{{"0", {"0", 0, 1}}, {"2", {"2", 1, 0}}}, {{"1", {"1", 0, 0}}, {"3", {"3", 1, 1}}} };

    int counter = 0;
    for (int reg = 0; reg < kNumRegions; reg++) {
      for (int rep = 0; rep < kNumReplicas; rep++) {
        for (int p = 0; p < kNumPartitions; p++) {
          auto id = MakeMachineId(reg, rep, p);
          auto config = configs[counter++];
          test_configs_[id] = config;
          auto& slog = test_slogs_.emplace(id, config).first->second;
          slog.AddServerAndClient();
          slog.AddSimpleMetadataForwarder();
          slog.AddMultiHomeOrderer();
          slog.AddSequencer();
          slog.AddLogManagers();
          slog.AddScheduler();
          slog.AddLocalPaxos();
          // One region is selected to globally order the multihome batches
          if (config->leader_region_for_multi_home_ordering() == config->local_region()) {
            slog.AddGlobalPaxos();
          }

          for (int k = 0; k < kDataItems; k++) {
            const auto& item = data[p][k];
            slog.Data(Key(item.first), Record(item.second));
          }
        }
      }
    }

    auto main_reg = std::get<1>(param);
    auto main_rep = std::get<2>(param);
    auto main_part = std::get<3>(param);
    main_ = MakeMachineId(main_reg, main_rep, main_part);
    CHECK(test_slogs_.find(main_) != test_slogs_.end()) << "Cannot find machine " << MACHINE_ID_STR(main_);

    for (auto& [_, slog] : test_slogs_) {
      slog.StartInNewThreads();
    }
  }

  Transaction SendAndReceiveResult(Transaction* txn) {
    auto it = test_slogs_.find(main_);
    CHECK(it != test_slogs_.end());
    it->second.SendTxn(txn);
    return it->second.RecvTxnResult();
  }

  // Send the transaction and check the result using the given callback function
  // for all machines one by one.
  void SendAndCheckAllOneByOne(Transaction* txn, std::function<void(Transaction)> cb) {
    for (int reg = 1; reg < kNumRegions; reg++) {
      for (int p = 1; p < kNumPartitions; p++) {
        auto it = test_slogs_.find(MakeMachineId(reg, p % kNumReplicas, p));
        CHECK(it != test_slogs_.end());

        auto copied_txn = new Transaction(*txn);

        it->second.SendTxn(copied_txn);
        auto resp = it->second.RecvTxnResult();
        LOG(INFO) << "Reg: " << reg << " P: " << p;

        cb(resp);
      }
    }
  }

 private:
  unordered_map<MachineId, ConfigurationPtr> test_configs_;
  unordered_map<MachineId, TestSlog> test_slogs_;
  MachineId main_;
};

TEST_P(E2ETest, SingleHomeSinglePartition) {

  auto txn1 = MakeTransaction({{"0", KeyType::WRITE}}, {{"SET", "0", "newA"}});
  auto params = GetParam();

  txn1->mutable_internal()->set_is_one_shot_txn(std::get<4>(params));

  auto resp = SendAndReceiveResult(txn1);
  ASSERT_EQ(resp.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(resp.internal().type(), TransactionType::SINGLE_HOME);
  ASSERT_EQ(resp.keys_size(), 1);
  ASSERT_EQ(TxnValueEntry(resp, "0").value(), "0");
  ASSERT_EQ(TxnValueEntry(resp, "0").new_value(), "newA");

  auto txn2 = MakeTransaction({{"0", KeyType::READ}}, {{"GET", "0"}});
  SendAndCheckAllOneByOne(txn2, [this](Transaction resp) {
    ASSERT_EQ(resp.status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(resp.internal().type(), TransactionType::SINGLE_HOME);
    ASSERT_EQ(resp.keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(resp, "0").value(), "newA");
  });
}

TEST_P(E2ETest, SingleHomeMultiPartition) {
  auto txn1 = MakeTransaction({{"0", KeyType::READ}, {"1", KeyType::WRITE}}, {{"GET", "0"}, {"SET", "1", "newB"}});
  auto params = GetParam();

  txn1->mutable_internal()->set_is_one_shot_txn(std::get<4>(params));
  auto resp = SendAndReceiveResult(txn1);
  ASSERT_EQ(resp.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(resp.internal().type(), TransactionType::SINGLE_HOME);
  ASSERT_EQ(resp.keys().size(), 2);
  ASSERT_EQ(TxnValueEntry(resp, "0").value(), "0");
  ASSERT_EQ(TxnValueEntry(resp, "1").new_value(), "newB");

  auto txn2 = MakeTransaction({{"1", KeyType::READ}}, {{"GET", "1"}});
  SendAndCheckAllOneByOne(txn2, [this](Transaction resp) {
    ASSERT_EQ(resp.status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(resp.internal().type(), TransactionType::SINGLE_HOME);
    ASSERT_EQ(resp.keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(resp, "1").value(), "newB");
  });
}

TEST_P(E2ETest, MultiHomeMultiPartitionTwoKeys) {
  auto txn1 = MakeTransaction({{"1", KeyType::WRITE}, {"2", KeyType::WRITE}}, {{"SET", "1", "newB"}, {"SET", "2", "newC"}});
  auto params = GetParam();

  txn1->mutable_internal()->set_is_one_shot_txn(std::get<4>(params));
  auto resp = SendAndReceiveResult(txn1);
  ASSERT_EQ(resp.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(resp.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
  ASSERT_EQ(resp.keys().size(), 2);
  ASSERT_EQ(TxnValueEntry(resp, "1").new_value(), "newB");
  ASSERT_EQ(TxnValueEntry(resp, "2").new_value(), "newC");
  /*
  auto txn2 = MakeTransaction({{"1", KeyType::READ}}, {{"GET", "1"}});
  SendAndCheckAllOneByOne(txn2, [this](Transaction resp) {
    ASSERT_EQ(resp.status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(resp.internal().type(), TransactionType::SINGLE_HOME);
    ASSERT_EQ(resp.keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(resp, "1").value(), "newB");
  });
   */
}

TEST_P(E2ETest, MultiHomeSinglePartition) {
  auto txn1 = MakeTransaction({{"1", KeyType::READ}, {"3", KeyType::WRITE}}, {{"GET", "1"}, {"SET", "3", "newC"}});
  auto params = GetParam();

  txn1->mutable_internal()->set_is_one_shot_txn(std::get<4>(params));
  auto resp = SendAndReceiveResult(txn1);
  ASSERT_EQ(resp.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(resp.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
  ASSERT_EQ(resp.keys().size(), 2);
  ASSERT_EQ(TxnValueEntry(resp, "1").value(), "1");
  ASSERT_EQ(TxnValueEntry(resp, "3").new_value(), "newC");

  auto txn2 = MakeTransaction({{"3", KeyType::READ}}, {{"GET", "3"}});

  SendAndCheckAllOneByOne(txn2, [this](Transaction resp) {
    ASSERT_EQ(resp.status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(resp.internal().type(), TransactionType::SINGLE_HOME);
    ASSERT_EQ(resp.keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(resp, "3").value(), "newC");
  });
}

TEST_P(E2ETest, MultiHomeMultiPartition) {
  auto txn = MakeTransaction({{"0", KeyType::READ}, {"1", KeyType::READ}, {"2", KeyType::READ}, {"3", KeyType::READ}});
  auto params = GetParam();

  txn->mutable_internal()->set_is_one_shot_txn(std::get<4>(params));
  auto resp = SendAndReceiveResult(txn);
  ASSERT_EQ(resp.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(resp.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
  ASSERT_EQ(resp.keys().size(), 4);
  ASSERT_EQ(TxnValueEntry(resp, "0").value(), "0");
  ASSERT_EQ(TxnValueEntry(resp, "1").value(), "1");
  ASSERT_EQ(TxnValueEntry(resp, "2").value(), "2");
  ASSERT_EQ(TxnValueEntry(resp, "3").value(), "3");
}

TEST_P(E2ETest, MultiHomeMultiPartitionWrites) {
  auto txn = MakeTransaction({{"0", KeyType::WRITE}, {"1", KeyType::WRITE}, {"2", KeyType::WRITE}, {"3", KeyType::WRITE}},
                             {{"SET", "0", "new0"}, {"SET", "1", "new1"}, {"SET", "2", "new2"}, {"SET", "3", "new3"}});
  auto params = GetParam();

  txn->mutable_internal()->set_is_one_shot_txn(std::get<4>(params));
  auto resp = SendAndReceiveResult(txn);
  ASSERT_EQ(resp.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(resp.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
  ASSERT_EQ(resp.keys().size(), 4);
  ASSERT_EQ(TxnValueEntry(resp, "0").value(), "0");
  ASSERT_EQ(TxnValueEntry(resp, "0").new_value(), "new0");
  ASSERT_EQ(TxnValueEntry(resp, "1").value(), "1");
  ASSERT_EQ(TxnValueEntry(resp, "1").new_value(), "new1");
  ASSERT_EQ(TxnValueEntry(resp, "2").value(), "2");
  ASSERT_EQ(TxnValueEntry(resp, "2").new_value(), "new2");
  ASSERT_EQ(TxnValueEntry(resp, "3").value(), "3");
  ASSERT_EQ(TxnValueEntry(resp, "3").new_value(), "new3");
}

/*
#ifdef ENABLE_REMASTER
TEST_P(E2ETest, RemasterTxn) {
  auto remaster_txn = MakeTransaction({{"A", KeyType::WRITE}}, {}, 1);

  auto remaster_txn_resp = SendAndReceiveResult(remaster_txn);

  ASSERT_EQ(TransactionStatus::COMMITTED, remaster_txn_resp.status());

#ifdef REMASTER_PROTOCOL_COUNTERLESS
  ASSERT_EQ(TransactionType::MULTI_HOME_OR_LOCK_ONLY, remaster_txn_resp.internal().type());
#else // REMASTER_PROTOCOL_COUNTERLESS
  ASSERT_EQ(TransactionType::SINGLE_HOME, remaster_txn_resp.internal().type());
#endif

  auto txn = MakeTransaction({{"A"}, {"X"}});

  // Since replication factor is set to 2 for all tests in this file, it is
  // guaranteed that this txn will see the changes made by the remaster txn
  auto txn_resp = SendAndReceiveResult(txn);
  ASSERT_EQ(txn_resp.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(txn_resp.internal().type(), TransactionType::SINGLE_HOME);  // used to be MH
  ASSERT_EQ(txn_resp.keys().size(), 2);
  ASSERT_EQ(TxnValueEntry(txn_resp, "A").value(), "valA");
  ASSERT_EQ(TxnValueEntry(txn_resp, "X").value(), "valX");
}
#endif

TEST_P(E2ETest, AbortTxnBadCommand) {
  // Multi-partition transaction where one of the partition will abort
  auto aborted_txn = MakeTransaction({{"A"}, {"B", KeyType::WRITE}}, {{"SET", "B", "notB"}, {"EQ", "A", "notA"}});

  auto aborted_txn_resp = SendAndReceiveResult(aborted_txn);
  ASSERT_EQ(TransactionStatus::ABORTED, aborted_txn_resp.status());
  ASSERT_EQ(TransactionType::SINGLE_HOME, aborted_txn_resp.internal().type());

  auto txn = MakeTransaction({{"B"}}, {{"GET", "B"}});

  auto txn_resp = SendAndReceiveResult(txn);
  ASSERT_EQ(txn_resp.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(txn_resp.internal().type(), TransactionType::SINGLE_HOME);
  // Value of B must not change because the previous txn was aborted
  ASSERT_EQ(TxnValueEntry(txn_resp, "B").value(), "valB");
}

TEST_P(E2ETest, AbortTxnEmptyKeySets) {
  // Multi-partition transaction where one of the partition will abort
  auto aborted_txn = MakeTransaction({});

  auto aborted_txn_resp = SendAndReceiveResult(aborted_txn);
  ASSERT_EQ(TransactionStatus::ABORTED, aborted_txn_resp.status());
  ASSERT_EQ(TransactionType::UNKNOWN, aborted_txn_resp.internal().type());
}
*/
// class E2ETestBypassMHOrderer : public E2ETest {
//   internal::Configuration CustomConfig() final {
//     internal::Configuration config;
//     config.set_bypass_mh_orderer(true);
// #ifdef LOCK_MANAGER_DDR
//     config.set_ddr_interval(10);
// #else
//     config.set_synchronized_batching(true);
//     // Artificially offset the txn timestamp far into the future to avoid abort
//     config.set_timestamp_buffer_us(500000);
// #endif
//     return config;
//   }
// };

// TEST_P(E2ETestBypassMHOrderer, MultiHomeSinglePartition) {
//   auto txn1 = MakeTransaction({{"A", KeyType::READ}, {"C", KeyType::WRITE}}, {{"GET", "A"}, {"SET", "C", "newC"}});
//   auto resp = SendAndReceiveResult(txn1);
//   ASSERT_EQ(resp.status(), TransactionStatus::COMMITTED);
//   ASSERT_EQ(resp.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
//   ASSERT_EQ(resp.keys().size(), 2);
//   ASSERT_EQ(TxnValueEntry(resp, "A").value(), "valA");
//   ASSERT_EQ(TxnValueEntry(resp, "C").new_value(), "newC");

//   auto txn2 = MakeTransaction({{"C", KeyType::READ}}, {{"GET", "C"}});
//   SendAndCheckAllOneByOne(txn2, [this](Transaction resp) {
//     ASSERT_EQ(resp.status(), TransactionStatus::COMMITTED);
//     ASSERT_EQ(resp.internal().type(), TransactionType::SINGLE_HOME);
//     ASSERT_EQ(resp.keys_size(), 1);
//     ASSERT_EQ(TxnValueEntry(resp, "C").value(), "newC");
//   });
// }

// TEST_P(E2ETestBypassMHOrderer, MultiHomeMultiPartition) {
//   auto txn = MakeTransaction({{"A", KeyType::READ}, {"X", KeyType::READ}, {"C", KeyType::READ}});
//   auto resp = SendAndReceiveResult(txn);
//   ASSERT_EQ(resp.status(), TransactionStatus::COMMITTED);
//   ASSERT_EQ(resp.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
//   ASSERT_EQ(resp.keys().size(), 3);
//   ASSERT_EQ(TxnValueEntry(resp, "A").value(), "valA");
//   ASSERT_EQ(TxnValueEntry(resp, "X").value(), "valX");
//   ASSERT_EQ(TxnValueEntry(resp, "C").value(), "valC");
// }

INSTANTIATE_TEST_SUITE_P(AllE2ETests, E2ETest,
                         testing::Combine(testing::Values(false, true), testing::Range(0, kNumRegions),
                                          testing::Range(0, 1), testing::Range(0, kNumPartitions), testing::Values(false)),
                         [](const testing::TestParamInfo<E2ETest::ParamType>& info) {
                           bool local_sync_rep = std::get<0>(info.param);
                           auto reg = std::get<1>(info.param);
                           auto rep = std::get<2>(info.param);
                           auto part = std::get<3>(info.param);
                           auto one_shot_txns = std::get<4>(info.param);

                           std::string out;
                           out += (local_sync_rep ? "Sync" : "Async");
                           out += "_";
                           out += std::to_string(reg) + "_";
                           out += std::to_string(rep) + "_";
                           out += std::to_string(part) + "_";
                           out += std::to_string(one_shot_txns);

                           return out;
                         });

const int kTxnGeneratorNumRegions = 2;
const int kTxnGeneratorNumReplicas = 1;
const int kTxnGeneratorNumPartitions = 1;

class E2ETxnGeneratorDetockTest : public ::testing::TestWithParam<tuple<bool, int, int, int, int>> {
 protected:
  virtual internal::Configuration CustomConfig() { return internal::Configuration(); }

  void SetUp() {
    auto param = GetParam();
    bool local_sync_rep = std::get<0>(param);
    int clients_per_txn_gen = std::get<1>(param);
    int multi_part_percentage = std::get<2>(param);
    int multi_home_percentage = std::get<3>(param);
    int num_generators = std::get<4>(param);
    auto is_detock = false;
    auto custom_config = CustomConfig();
    /*
    custom_config.add_replication_order("1");
    custom_config.add_replication_order("0");
     */
    //custom_config.set_replication_factor(2);
    custom_config.set_num_workers(3);
    custom_config.set_num_log_managers(1);
    custom_config.set_ddr_interval(1);

    custom_config.set_forwarder_batch_duration(0);
    custom_config.set_bypass_mh_orderer(is_detock);
    custom_config.set_fs_latency_interval(200);
    custom_config.set_avg_latency_window_size(10);
    custom_config.set_timestamp_buffer_us(0);

    custom_config.set_synchronized_batching(is_detock);
    custom_config.set_sequencer_batch_duration(0);
    custom_config.set_sequencer_batch_size(100);
    custom_config.set_sequencer_rrr(true);
    custom_config.set_txn_issuing_delay(10);
    //custom_config.set_one_shot_txns(true);
    //custom_config.set_execution_type(internal::ExecutionType::TPC_C);

    //custom_config.mutable_tpcc_partitioning()->set_warehouses(kTxnGeneratorNumRegions*kTxnGeneratorNumPartitions*2);
    custom_config.mutable_simple_partitioning()->set_num_records(125000);
    custom_config.mutable_simple_partitioning()->set_record_size_bytes(100);

    //custom_config.mutable_network_latency();

    auto configs =
        MakeTestConfigurations("e2eDetock", kTxnGeneratorNumRegions, kTxnGeneratorNumReplicas, kTxnGeneratorNumPartitions, custom_config, local_sync_rep);

    std::shared_ptr<MetadataInitializer> metadata_initializer;


    /*
    std::pair<Key, Record> data[kNumPartitions][kDataItems] = {{{"A", {"valA", 0, 0}}, {"C", {"valC", 1, 1}}},
                                                               {{"B", {"valB", 0, 1}}, {"X", {"valX", 1, 0}}}};
                                                               */
    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    string test_name;
    if (test_info) {
      test_name = test_info->name();
      std::istringstream tokenStream(test_name);
      std::getline(tokenStream, test_name, '/');
    }


    int counter = 0;
    for (int reg = 0; reg < kTxnGeneratorNumRegions; reg++) {
      for (int rep = 0; rep < kTxnGeneratorNumReplicas; rep++) {
        for (int p = 0; p < kTxnGeneratorNumPartitions; p++) {
          auto id = MakeMachineId(reg, rep, p);
          auto config = configs[counter++];


          auto rate_limiter = std::make_shared<RateLimiter>(0);
          auto& slog = test_slogs_.emplace(id, TestSlog(config, false)).first->second;


          slog.AddServerAndClient();
          slog.AddSimpleMetadataForwarder();

          slog.AddSequencer();
          slog.AddLogManagers();
          slog.AddScheduler();
          slog.AddLocalPaxos();
          slog.AddMultiHomeOrderer();
          // One region is selected to globally order the multihome batches
          if (config->leader_region_for_multi_home_ordering() == config->local_region()) {
            slog.AddGlobalPaxos();
          }

          if (config->execution_type() == internal::ExecutionType::TPC_C){
            LOG(INFO) << "INITIALIZE TPCC";
            slog.InitializeTPCC();
            LOG(INFO) << "DONE";

          }
          if (p == 0){
            std::shared_ptr<ZipfDistribution> zipf_dist_vect;
            zipf_dist_vect.reset(new ZipfDistribution());

            for (int gen = 0; gen < num_generators; gen++){
              // Identify the metadata initializer
              switch (config->proto_config().partitioning_case()) {
                case internal::Configuration::kSimplePartitioning:
                  metadata_initializer = make_shared<SimpleMetadataInitializer>(config->num_regions(), config->num_partitions());
                  break;
                case internal::Configuration::kSimplePartitioning2:
                  metadata_initializer = make_shared<SimpleMetadataInitializer2>(config->num_regions(), config->num_partitions());
                  break;

                case internal::Configuration::kTpccPartitioning:
                  metadata_initializer =
                      make_shared<tpcc::TPCCMetadataInitializer>(config->num_regions(), config->num_partitions(), config->local_region());
                  break;
                default:
                  metadata_initializer = make_shared<ConstantMetadataInitializer>(0);
                  break;
              }

              if (config->proto_config().partitioning_case() == internal::Configuration::kTpccPartitioning) {
                auto workload_type = make_unique<slog::TPCCWorkload>(config, reg, rep, "",
                                                                     std::make_pair(gen + 1, num_generators));
                slog.AddTxnGenerator(move(workload_type), 0, clients_per_txn_gen, 30, rate_limiter);
              } else {
                //auto workload_type = make_unique<slog::BasicWorkload>(config, reg, rep, "", special_configs);
                /*
                //auto workload_type = make_unique<slog::BasicWorkload>(config, reg, rep, "", special_configs);
                std::stringstream workload;
                workload << "mp=" << multi_part_percentage << ",mh=" << multi_home_percentage << ",hot=" << 10000 << ",hot_records=" << 2 << ",rw=" << 100;
                auto special_configs = workload.str();

                auto workload_type = make_unique<slog::YCSBWorkload>(config, reg, rep, "", special_configs);
                auto mh_perc = workload_type->params().GetDouble("mh");
                LOG(INFO) << "MH PERC " << mh_perc;
                slog.AddTxnGenerator(move(workload_type), 0, clients_per_txn_gen, 30, rate_limiter);
                */

                std::stringstream workload;
                //workload << "mp=" << multi_part_percentage << ",mh=" << multi_home_percentage << ",hot=" << 10000 << ",hot_records=" << 2 << ",rw=" << 100;
                workload << "mh=" << multi_home_percentage << ",zipf=" << 0 << ",fi=0,mix=50_0_50";
                auto special_configs = workload.str();
                auto workload_type = make_unique<slog::YCSBWorkload>(config, reg, rep, "", special_configs, zipf_dist_vect);
                slog.AddTxnGenerator(move(workload_type), 0, clients_per_txn_gen, 30, rate_limiter);

              }
            }
          }
        }
      }
    }

    for (auto& [_, slog] : test_slogs_) {
      slog.StartSlogInNewThreads();
    }

    for (auto& [_, slog] : test_slogs_) {
      slog.StartTxnGeneratorInNewThreads();
    }
  }

  std::map<int, std::array<size_t, 4>> TxnGeneratorProgress(MachineId id){
    auto it = test_slogs_.find(id);
    CHECK(it != test_slogs_.end());
    return it->second.TxnGeneratorProgress();
  }

  bool TxnGeneratorRunning(MachineId id){
    auto it = test_slogs_.find(id);
    CHECK(it != test_slogs_.end());
    return it->second.TxnGeneratorRunning();
  }

 private:
  unordered_map<MachineId, TestSlog> test_slogs_;
};

TEST_P(E2ETxnGeneratorDetockTest, MultiClientDeployment) {

  auto progress_to_string = [](std::array<unsigned long, 4> progress) {
    stringstream progress_str;
    progress_str << "[" << progress[0] << ":" << progress[1] << ":" << progress[2] << ":" << progress[3] << "]";
    return progress_str.str();
  };
  unordered_map<MachineId, std::map<int, std::array<size_t, 4>>> last_txn_generator_progress;
  auto start = std::chrono::high_resolution_clock::now();
  auto periodic_check = std::chrono::high_resolution_clock::now();
  auto now = std::chrono::high_resolution_clock::now();
  while (true) {

    if (now > periodic_check + milliseconds(2000)){
      bool running = false;

      for (int reg = 0; reg < kTxnGeneratorNumRegions; reg++) {
        for (int rep = 0; rep < kTxnGeneratorNumReplicas; rep++) {
          for (int p = 0; p < kTxnGeneratorNumPartitions; p++) {
            auto machine_id = MakeMachineId(reg, rep, p);
            if (p == 0){
              auto txn_generator_progress = TxnGeneratorProgress(machine_id);

              int i = 0;
              for (auto & gen_progress : txn_generator_progress){
                auto progress = gen_progress.second;
                LOG(INFO) << "[" << reg << ":" << rep << ":" << p << ":" << gen_progress.first << "] [" << progress[0] << ":" << progress[1] << ":" << progress[2] << ":" << progress[3] << "]";

                 }
              last_txn_generator_progress[machine_id] = txn_generator_progress;



              running |= TxnGeneratorRunning(machine_id);
            }

          }
        }
      }
      if (!running){
        break;
      }
      periodic_check = now;
    }


    now = std::chrono::high_resolution_clock::now();
  }
}

INSTANTIATE_TEST_SUITE_P(AllE2ETests, E2ETxnGeneratorDetockTest,
                         testing::Combine(testing::Values(false), testing::Values(100),
                                          testing::Values(100), testing::Values(100), testing::Values(2)),
                         [](const testing::TestParamInfo<E2ETxnGeneratorDetockTest::ParamType>& info) {

                           bool local_sync_rep = std::get<0>(info.param);
                           auto num_clients = std::get<1>(info.param);

                           std::stringstream out;
                           out << (local_sync_rep ? "Sync" : "Async") << "_";
                           out << "Clients_" << num_clients << "_";
                           out << "workload_mp_" << std::get<2>(info.param) << "mh_" << std::get<3>(info.param) << "_" <<
                               "generators_" << std::get<4>(info.param);

                           return out.str();
                         });

int main(int argc, char* argv[]) {
  //FLAGS_v = 4;

  ::testing::InitGoogleTest(&argc, argv);
  google::InstallFailureSignalHandler();
  return RUN_ALL_TESTS();
}