#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/proto_utils.h"
#include "execution/tpcc/metadata_initializer.h"
#include "module/server.h"
#include "proto/api.pb.h"
#include "test/glog_test_utils.h"

#include "workload/skewed.h"
#include "workload/tpcc.h"
#include "workload/ycsb.h"

using namespace std;
using namespace slog;
using namespace glog;

const int kNumRegions = 2;
const int kNumReplicas = 1;
const int kNumPartitions = 2;
const int kNumMachines = kNumRegions * kNumReplicas * kNumPartitions;
const int kDataItems = 2;

// TODO - Add multi-region suport
class E2EGlogTest : public ::testing::TestWithParam<tuple<bool, int, int>> {
 protected:
  virtual internal::Configuration CustomConfig() { return internal::Configuration(); }

  void SetUp() {
    auto param = GetParam();
    bool local_sync_rep = std::get<0>(param);

    auto custom_config = CustomConfig();

    custom_config.set_epoch_duration(milliseconds(3000).count());
    custom_config.set_num_workers(2);
    custom_config.set_num_log_managers(2);

    custom_config.set_min_slots(2);
    custom_config.set_rounding_factor(1);
    custom_config.set_num_workers(2);
    custom_config.set_start_slots(20);

    custom_config.mutable_simple_partitioning()->set_num_records(2500000);
    custom_config.mutable_simple_partitioning()->set_record_size_bytes(100);
    custom_config.set_log_type(slog::internal::MULTI_SLOT);

    custom_config.set_num_stubs(1);

    auto configs =
        MakeGlogTestConfigurations("e2eGlog", kNumRegions, kNumReplicas, kNumPartitions, custom_config, local_sync_rep);

    std::pair<Key, Record> data[kNumPartitions][kDataItems] = {{{"0", {"0", 0, 0}}, {"2", {"2", 1, 0}}},
                                                               {{"1", {"1", 0, 0}}, {"3", {"3", 1, 0}}}};

    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    string test_name;
    if (test_info) {
      test_name = test_info->name();
      std::istringstream tokenStream(test_name);
      std::getline(tokenStream, test_name, '/');
    }

    for (int i = 0; i < 4; i++) {
      auto key = to_string(i);
      LOG(INFO) << key << " " << std::stoll(key) << " " << std::stoll(key) % kNumPartitions;
    }

    int counter = 0;
    for (int reg = 0; reg < kNumRegions; reg++) {
      for (int rep = 0; rep < kNumReplicas; rep++) {
        for (int p = 0; p < kNumPartitions; p++) {
          auto id = MakeMachineId(reg, rep, p);
          auto config = configs[counter++];

          auto& glog = test_glogs_.emplace(id, config).first->second;

          glog.AddServerAndClient();
          glog.AddSimpleMetadataForwarder();
          glog.AddSequencer();
          glog.AddLogManagers();
          glog.AddScheduler();
          glog.AddLocalPaxos();

          if (config->shrink_stubs()) {
            if (p == config->leader_partition_for_multi_home_ordering()) {
              glog.AddGlogStubs(1);
            }
          } else {
            glog.AddGlogStubs(1);
          }

          if (p == 0) {
            glog.AddGlog();
          }

          senders_.emplace(MakeMachineId(reg, rep, p), glog.NewSender());

          for (int k = 0; k < kDataItems; k++) {
            const auto& item = data[p][k];
            glog.Data(Key(item.first), Record(item.second));
          }
        }
      }
    }

    auto& order_glog = test_glogs_.emplace(numeric_limits<MachineId>::max(), configs[counter++]).first->second;
    order_glog.AddOrder();

    auto main_reg = std::get<1>(param);
    auto main_rep = std::get<2>(param);
    main_ = MakeMachineId(main_reg, main_rep, 0);
    CHECK(test_glogs_.find(main_) != test_glogs_.end()) << "Cannot find machine " << MACHINE_ID_STR(main_);

    for (auto& [_, glog] : test_glogs_) {
      glog.StartGlogInNewThreads();
    }
  }

  Transaction SendAndReceiveResult(Transaction* txn) {
    auto it = test_glogs_.find(main_);
    CHECK(it != test_glogs_.end());
    it->second.SendTxn(txn);
    return it->second.RecvTxnResult();
  }

 private:
  unordered_map<MachineId, TestGlog> test_glogs_;
  MachineId main_;
  unordered_map<MachineId, unique_ptr<Sender>> senders_;
};

TEST_P(E2EGlogTest, SingleHomeSinglePartition) {
  auto txn1 = MakeTransaction({{"0", KeyType::WRITE}}, {{"SET", "0", "newA"}});
  txn1->mutable_internal()->set_id(1);
  txn1->mutable_internal()->set_is_one_shot_txn(true);
  auto received_txn = SendAndReceiveResult(txn1);

  ASSERT_EQ(received_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(received_txn.internal().type(), TransactionType::SINGLE_HOME);
  ASSERT_EQ(received_txn.keys_size(), 1);
  ASSERT_EQ(TxnValueEntry(received_txn, "0").value(), "0");
  ASSERT_EQ(TxnValueEntry(received_txn, "0").new_value(), "newA");

}

TEST_P(E2EGlogTest, SingleHomeMultiPartition) {
  auto txn1 = MakeTransaction({{"0", KeyType::READ}, {"1", KeyType::WRITE}}, {{"GET", "0"}, {"SET", "1", "newB"}});
  txn1->mutable_internal()->set_id(1);
  txn1->mutable_internal()->set_is_one_shot_txn(true);

  auto received_txn = SendAndReceiveResult(txn1);

  ASSERT_EQ(received_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(received_txn.internal().type(), TransactionType::SINGLE_HOME);
  ASSERT_EQ(received_txn.keys().size(), 2);
  ASSERT_EQ(TxnValueEntry(received_txn, "0").value(), "0");
  ASSERT_EQ(TxnValueEntry(received_txn, "1").new_value(), "newB");

}

TEST_P(E2EGlogTest, MultiHomeSinglePartition) {
  auto txn1 =
      MakeTransaction({{"1", KeyType::WRITE}, {"3", KeyType::WRITE}}, {{"SET", "1", "newA"}, {"SET", "3", "newC"}});
  txn1->mutable_internal()->set_id(1);
  txn1->mutable_internal()->set_is_one_shot_txn(true);

  auto received_txn = SendAndReceiveResult(txn1);

  ASSERT_EQ(received_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(received_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
  ASSERT_EQ(received_txn.keys_size(), 2);
  ASSERT_EQ(TxnValueEntry(received_txn, "1").value(), "1");
  ASSERT_EQ(TxnValueEntry(received_txn, "1").new_value(), "newA");
  ASSERT_EQ(TxnValueEntry(received_txn, "3").value(), "3");
  ASSERT_EQ(TxnValueEntry(received_txn, "3").new_value(), "newC");


}

TEST_P(E2EGlogTest, MultiHomeMultiPartition) {
  auto txn1 =
      MakeTransaction({{"0", KeyType::WRITE}, {"1", KeyType::WRITE}, {"2", KeyType::WRITE}, {"3", KeyType::WRITE}},
                      {{"SET", "0", "newA"}, {"SET", "1", "newC"}, {"SET", "2", "newB"}, {"SET", "3", "newX"}});
  txn1->mutable_internal()->set_id(1);
  txn1->mutable_internal()->set_is_one_shot_txn(true);

  auto received_txn = SendAndReceiveResult(txn1);

  ASSERT_EQ(received_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(received_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
  ASSERT_EQ(received_txn.keys_size(), 4);
  ASSERT_EQ(TxnValueEntry(received_txn, "0").value(), "0");
  ASSERT_EQ(TxnValueEntry(received_txn, "0").new_value(), "newA");
  ASSERT_EQ(TxnValueEntry(received_txn, "1").value(), "1");
  ASSERT_EQ(TxnValueEntry(received_txn, "1").new_value(), "newC");
  ASSERT_EQ(TxnValueEntry(received_txn, "2").value(), "2");
  ASSERT_EQ(TxnValueEntry(received_txn, "2").new_value(), "newB");
  ASSERT_EQ(TxnValueEntry(received_txn, "3").value(), "3");
  ASSERT_EQ(TxnValueEntry(received_txn, "3").new_value(), "newX");


}

INSTANTIATE_TEST_SUITE_P(AllE2ETests, E2EGlogTest,
                         testing::Combine(testing::Values(false), testing::Values(0), testing::Values(0)),
                         [](const testing::TestParamInfo<E2EGlogTest::ParamType>& info) {
                           bool local_sync_rep = std::get<0>(info.param);
                           auto reg = std::get<1>(info.param);
                           auto rep = std::get<2>(info.param);
                           std::string out;
                           out += (local_sync_rep ? "Sync" : "Async");
                           out += "_";
                           out += std::to_string(reg) + "_";
                           out += std::to_string(rep);
                           return out;
                         });

const int kTxnGeneratorNumRegions = 2;
const int kTxnGeneratorNumReplicas = 1;
const int kTxnGeneratorNumPartitions = 1;

/**
 * E2E test simulating a complete execution including transaction generators
 *
 * The test includes 5 parameters:
 *      Synchrony - ??
 *      Clients per Generator - Number of logical clients that the generator simulates
 *      Multi-Partition Percentage - Percentage of multi-partition transactions
 *      Multi-Home Percentage - Percentage of multi-home transactions
 *      Log Type - Type of log to be used for GLOG (types can be found in proto)
 *      Number of Generators - Number of generators per region
 *
 */
class E2ETxnGeneratorGlogTest
    : public ::testing::TestWithParam<tuple<bool, int, int, int, slog::internal::LogType, int>> {
 protected:
  virtual internal::Configuration CustomConfig() { return internal::Configuration(); }

  void SetUp() {
    auto param = GetParam();
    bool local_sync_rep = std::get<0>(param);
    int clients_per_txn_gen = std::get<1>(param);
    int multi_part_percentage = std::get<2>(param);
    int multi_home_percentage = std::get<3>(param);
    auto log_type = std::get<4>(param);
    int num_generators = std::get<5>(param);

    auto custom_config = CustomConfig();
    /*
    custom_config.add_replication_order("1");
    custom_config.add_replication_order("0");
     */
    // custom_config.set_replication_factor(2);
    custom_config.set_epoch_duration(milliseconds(60000).count());
    custom_config.set_start_slots(60000);
    custom_config.set_min_slots(100);
    custom_config.set_num_workers(1);

    // custom_config.mutable_simple_partitioning()->set_num_records(kTxnGeneratorNumRegions*kTxnGeneratorNumPartitions*num_generators*clients_per_txn_gen*10);

    custom_config.mutable_simple_partitioning()->set_num_records(1000*kTxnGeneratorNumRegions*kTxnGeneratorNumPartitions);
    custom_config.mutable_simple_partitioning()->set_record_size_bytes(100);

    //custom_config.set_execution_type(internal::ExecutionType::TPC_C);
    //custom_config.mutable_tpcc_partitioning()->set_warehouses(kTxnGeneratorNumRegions*kTxnGeneratorNumPartitions);

    custom_config.set_log_type(log_type);

    // custom_config.set_one_shot_txns(true);
    //  Delays
    custom_config.add_network_latency(0.0);
    custom_config.add_network_latency(5.0);
    custom_config.add_network_latency(5.0);
    custom_config.add_network_latency(0.0);

    custom_config.set_shrink_stubs(true);
    custom_config.set_num_stubs(1);

    custom_config.mutable_metric_options()->set_stub_goodput_sample(100);

    custom_config.add_enabled_events(ENTER_SERVER);
    auto configs = MakeGlogTestConfigurations("e2eZiplog", kTxnGeneratorNumRegions, kTxnGeneratorNumReplicas,
                                              kTxnGeneratorNumPartitions, custom_config, local_sync_rep);

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

          std::stringstream workload;
          workload << "mp=" << multi_part_percentage << ",mh=" << multi_home_percentage << ",hot=" << 10
                   << ",hot_records=" << 2;
          auto special_configs = workload.str();
          auto rate_limiter = std::make_shared<RateLimiter>(0);
          auto& glog = test_glogs_.emplace(id, TestGlog(config, true)).first->second;

          glog.AddServerAndClient();
          glog.AddSimpleMetadataForwarder();
          glog.AddSequencer();
          glog.AddLogManagers();
          glog.AddScheduler();
          glog.AddLocalPaxos();
          if (config->shrink_stubs()) {
            if (p == config->leader_partition_for_multi_home_ordering()) {
              glog.AddGlogStubs(1);
            }
          } else {
            glog.AddGlogStubs(1);
          }

          if (p == 0) {
            std::shared_ptr<ZipfDistribution> zipf_dist_vect;
            zipf_dist_vect.reset(new ZipfDistribution());

            glog.AddGlog();
            if (reg == 0) {

              for (int gen = 0; gen < num_generators; gen++) {
                // Identify the metadata initializer
                switch (config->proto_config().partitioning_case()) {
                  case internal::Configuration::kSimplePartitioning:
                    metadata_initializer =
                        make_shared<SimpleMetadataInitializer>(config->num_regions(), config->num_partitions());
                    break;
                  case internal::Configuration::kSimplePartitioning2:
                    metadata_initializer =
                        make_shared<SimpleMetadataInitializer2>(config->num_regions(), config->num_partitions());
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

                  auto workload_type =
                      make_unique<slog::TPCCWorkload>(config, reg, rep, "", std::make_pair(gen + 1, num_generators));
                  glog.AddTxnGenerator(move(workload_type), 0, clients_per_txn_gen, 1200, rate_limiter);
                } else {
                  std::stringstream workload;
                  //workload << "mp=" << multi_part_percentage << ",mh=" << multi_home_percentage << ",hot=" << 10000 << ",hot_records=" << 2 << ",rw=" << 100;
                  workload << "mh=" << multi_home_percentage << ",zipf=" << 0 << ",fi=0,mix=50_0_50";
                  auto special_configs = workload.str();
                  auto workload_type = make_unique<slog::YCSBWorkload>(config, reg, rep, "", special_configs, zipf_dist_vect);

                  glog.AddTxnGenerator(move(workload_type), 0, clients_per_txn_gen, 60, rate_limiter);
                }
            }
            }

          }
        }
      }
    }

    auto& order_glog = test_glogs_.emplace(numeric_limits<MachineId>::max(), configs[counter++]).first->second;
    order_glog.AddOrder();

    for (auto& [_, glog] : test_glogs_) {
      glog.StartGlogInNewThreads();
    }

    for (auto& [_, glog] : test_glogs_) {
      glog.StartTxnGeneratorInNewThreads();
    }
  }

  std::map<int, std::array<size_t, 4>> TxnGeneratorProgress(MachineId id) {
    auto it = test_glogs_.find(id);
    CHECK(it != test_glogs_.end());
    return it->second.TxnGeneratorProgress();
  }

  milliseconds GetEpochDuration() {
    CHECK(!test_glogs_.empty()) << "No glog tests to get the duration";
    return test_glogs_.begin()->second.GetEpochDuration();
  }

  bool TxnGeneratorRunning(MachineId id) {
    auto it = test_glogs_.find(id);
    CHECK(it != test_glogs_.end());
    return it->second.TxnGeneratorRunning();
  }

 private:
  unordered_map<MachineId, TestGlog> test_glogs_;
};

TEST_P(E2ETxnGeneratorGlogTest, MultiClientDeployment) {
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
    // Only start counting when the epoch duration has passed
    if (now > periodic_check + milliseconds(2000)) {
      bool running = false;

      for (int reg = 0; reg < kTxnGeneratorNumRegions; reg++) {
        for (int rep = 0; rep < kTxnGeneratorNumReplicas; rep++) {
          for (int p = 0; p < kTxnGeneratorNumPartitions; p++) {
            auto machine_id = MakeMachineId(reg, rep, p);
            if (p == 0 && reg == 0) {
              auto txn_generator_progress = TxnGeneratorProgress(machine_id);

              int i = 0;
              for (auto& gen_progress : txn_generator_progress) {
                auto progress = gen_progress.second;
                LOG(INFO) << "[" << reg << ":" << rep << ":" << p << ":" << gen_progress.first << "] [" << progress[0]
                          << ":" << progress[1] << ":" << progress[2] << ":" << progress[3] << "]";
              }

              last_txn_generator_progress[machine_id] = txn_generator_progress;

              running |= TxnGeneratorRunning(machine_id);
            }
          }
        }
      }
      if (!running) {
        break;
      }
      periodic_check = now;
    }

    now = std::chrono::high_resolution_clock::now();
  }
}

INSTANTIATE_TEST_SUITE_P(AllE2ETests, E2ETxnGeneratorGlogTest,
                         testing::Combine(testing::Values(false), testing::Values(2), testing::Values(0),
                                          testing::Values(5), testing::Values(slog::internal::MULTI_SLOT),
                                          testing::Values(2)),
                         [](const testing::TestParamInfo<E2ETxnGeneratorGlogTest::ParamType>& info) {
                           bool local_sync_rep = std::get<0>(info.param);
                           auto num_clients = std::get<1>(info.param);

                           std::stringstream out;
                           out << (local_sync_rep ? "Sync" : "Async") << "_";
                           out << "Clients_" << num_clients << "_";
                           out << "workload_mp_" << std::get<2>(info.param) << "mh_" << std::get<3>(info.param) << "_";
                           out << "log_type_" << (std::get<4>(info.param) == 0 ? "CLIENT_RATE" : "MULTI_SLOT") << "_";
                           out << "generators_" << std::get<5>(info.param) << "_";

                           return out.str();
                         });

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);

  //FLAGS_v = 3;
  google::InstallFailureSignalHandler();
  return RUN_ALL_TESTS();
}
