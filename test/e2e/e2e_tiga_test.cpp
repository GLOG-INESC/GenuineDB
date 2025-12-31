#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/proto_utils.h"
#include "execution/tpcc/metadata_initializer.h"
#include "module/server.h"
#include "test/tiga_test_utils.h"

#include "workload/skewed.h"
#include "workload/tpcc.h"
#include "workload/ycsb.h"

using namespace std;
using namespace slog;
using namespace tiga;

const int kNumRegions = 2;
const int kNumReplicas = 1;
const int kNumPartitions = 1;
const int kNumMachines = kNumRegions * kNumReplicas * kNumPartitions;
const int kDataItems = 1;

// TODO - Add multi-region suport
class E2ETigaTest : public ::testing::TestWithParam<tuple<bool, int, int>> {
 protected:
  virtual internal::Configuration CustomConfig() { return internal::Configuration(); }

  void SetUp() {
    auto param = GetParam();
    bool local_sync_rep = std::get<0>(param);

    auto custom_config = CustomConfig();

    custom_config.set_num_workers(1);

    custom_config.mutable_simple_partitioning()->set_num_records(2500000);
    custom_config.mutable_simple_partitioning()->set_record_size_bytes(100);

    custom_config.add_network_latency(0.1);
    custom_config.add_network_latency(5.0);
    custom_config.add_network_latency(5.0);
    custom_config.add_network_latency(0.1);

    auto configs =
        MakeTigaTestConfigurations("e2eTiga", kNumRegions, kNumReplicas, kNumPartitions, custom_config, local_sync_rep);

    std::map<RegionId, std::map<PartitionId, std::vector<std::pair<Key, Record>>>> data;

    data[0][0].push_back( {"0", {"0", 0, 0}});
    data[1][0].push_back( {"1", {"1", 1, 0}});


    const ::testing::TestInfo* const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    string test_name;
    if (test_info) {
      test_name = test_info->name();
      std::istringstream tokenStream(test_name);
      std::getline(tokenStream, test_name, '/');
    }

    int counter = 0;
    for (int reg = 0; reg < kNumRegions; reg++) {
      for (int rep = 0; rep < kNumReplicas; rep++) {
        for (int p = 0; p < kNumPartitions; p++) {
          auto id = MakeMachineId(reg, rep, p);
          auto config = configs[counter++];

          auto& tiga = test_tiga_.emplace(id, config).first->second;

          tiga.AddServerAndClient();
          tiga.AddCoordinator();
          tiga.AddAcceptor();
          tiga.AddScheduler();

          senders_.emplace(MakeMachineId(reg, rep, p), tiga.NewSender());

          for (auto & item : data[reg][p]) {
            tiga.Data(Key(item.first), Record(item.second));
          }

        }
      }
    }


    auto main_reg = std::get<1>(param);
    auto main_rep = std::get<2>(param);
    main_ = MakeMachineId(main_reg, main_rep, 0);
    CHECK(test_tiga_.find(main_) != test_tiga_.end()) << "Cannot find machine " << MACHINE_ID_STR(main_);

    for (auto& [_, tiga] : test_tiga_) {
      tiga.StartTigaInNewThreads();
    }
  }

  Transaction SendAndReceiveResult(Transaction* txn) {
    auto it = test_tiga_.find(main_);
    CHECK(it != test_tiga_.end());
    it->second.SendTxn(txn);
    return it->second.RecvTxnResult();
  }


 private:
  unordered_map<MachineId, TestTiga> test_tiga_;
  MachineId main_;
  unordered_map<MachineId, unique_ptr<Sender>> senders_;
};

TEST_P(E2ETigaTest, SingleHomeSinglePartition) {
  auto txn1 = MakeTransaction({{"0", KeyType::WRITE, 0}}, {{"SET", "0", "newA"}});
  txn1->mutable_internal()->set_id(1);
  txn1->mutable_internal()->set_is_one_shot_txn(true);
  auto received_txn = SendAndReceiveResult(txn1);

  ASSERT_EQ(received_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(received_txn.internal().type(), TransactionType::SINGLE_HOME);
  ASSERT_EQ(received_txn.keys_size(), 1);
  ASSERT_EQ(TxnValueEntry(received_txn, "0").value(), "0");
  ASSERT_EQ(TxnValueEntry(received_txn, "0").new_value(), "newA");

}

TEST_P(E2ETigaTest, MultiHomeSinglePartition) {
  auto txn1 = MakeTransaction({{"0", KeyType::WRITE, 0}, {"1", KeyType::WRITE, 1}}, {{"SET", "0", "new0"}, {"SET", "1", "new1"}});
  txn1->mutable_internal()->set_id(1);
  txn1->mutable_internal()->set_is_one_shot_txn(true);
  auto received_txn = SendAndReceiveResult(txn1);

  ASSERT_EQ(received_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(received_txn.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
  ASSERT_EQ(received_txn.keys_size(), 2);
  ASSERT_EQ(TxnValueEntry(received_txn, "0").value(), "0");
  ASSERT_EQ(TxnValueEntry(received_txn, "0").new_value(), "new0");
  ASSERT_EQ(TxnValueEntry(received_txn, "1").value(), "1");
  ASSERT_EQ(TxnValueEntry(received_txn, "1").new_value(), "new1");
}
  /*

TEST_P(E2ETigaTest, SingleHomeMultiPartition) {
  auto txn1 = MakeTransaction({{"0", KeyType::READ}, {"1", KeyType::WRITE}}, {{"GET", "0"}, {"SET", "1", "newB"}});
  txn1->mutable_internal()->set_id(1);
  txn1->mutable_internal()->set_is_one_shot_txn(true);

  auto received_txn = SendAndReceiveResult(txn1);

  ASSERT_EQ(received_txn.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(received_txn.internal().type(), TransactionType::SINGLE_HOME);
  ASSERT_EQ(received_txn.keys().size(), 2);
  ASSERT_EQ(TxnValueEntry(received_txn, "0").value(), "0");
  ASSERT_EQ(TxnValueEntry(received_txn, "1").new_value(), "newB");

sleep(4);

  auto txn2 = MakeTransaction({{"B", KeyType::READ}}, {{"GET", "B"}});
  txn2->mutable_internal()->set_id(2);
  SendAndCheckAllOneByOne(txn2, [this](slog::api::Response* resp) {
    ASSERT_TRUE(resp != nullptr);
    ASSERT_EQ(2, resp->stream_id());

    auto received_txn = resp->mutable_txn()->release_txn();
    ASSERT_EQ(received_txn->status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(received_txn->internal().type(), TransactionType::SINGLE_HOME);
    ASSERT_EQ(received_txn->keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(*received_txn, "B").value(), "newB");
  });
}

   */

INSTANTIATE_TEST_SUITE_P(AllE2ETests, E2ETigaTest,
                         testing::Combine(testing::Values(false), testing::Values(0), testing::Values(0)),
                         [](const testing::TestParamInfo<E2ETigaTest::ParamType>& info) {
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
const int kTxnGeneratorNumPartitions = 2;

class E2ETxnGeneratorTigaTest
    : public ::testing::TestWithParam<tuple<bool, int, int, int, int>> {
 protected:
  virtual internal::Configuration CustomConfig() { return internal::Configuration(); }

  void SetUp() {
    auto param = GetParam();
    bool local_sync_rep = std::get<0>(param);
    int clients_per_txn_gen = std::get<1>(param);
    int multi_part_percentage = std::get<2>(param);
    int multi_home_percentage = std::get<3>(param);
    int num_generators = std::get<4>(param);

    auto custom_config = CustomConfig();

    custom_config.set_num_workers(1);

    // custom_config.mutable_simple_partitioning()->set_num_records(kTxnGeneratorNumRegions*kTxnGeneratorNumPartitions*num_generators*clients_per_txn_gen*10);

    custom_config.mutable_simple_partitioning()->set_num_records(1000*kTxnGeneratorNumRegions*kTxnGeneratorNumPartitions);
    custom_config.mutable_simple_partitioning()->set_record_size_bytes(100);

    //custom_config.set_execution_type(internal::ExecutionType::TPC_C);
    //custom_config.mutable_tpcc_partitioning()->set_warehouses(kTxnGeneratorNumRegions*kTxnGeneratorNumPartitions);


    custom_config.set_one_shot_txns(true);

    //  Delays
    custom_config.add_network_latency(0.0);
    custom_config.add_network_latency(5.0);
    custom_config.add_network_latency(5.0);
    custom_config.add_network_latency(0.0);


    auto configs = MakeTigaTestConfigurations("e2eTiga", kTxnGeneratorNumRegions, kTxnGeneratorNumReplicas,
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
          auto& tiga = test_tiga_.emplace(id, TestTiga(config, false, true)).first->second;

          tiga.AddServerAndClient();
          tiga.AddCoordinator();
          tiga.AddAcceptor();
          tiga.AddScheduler();

          if (p == 0) {
            std::shared_ptr<ZipfDistribution> zipf_dist_vect;
            zipf_dist_vect.reset(new ZipfDistribution());


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
                  tiga.AddTxnGenerator(move(workload_type), 0, clients_per_txn_gen, 1200, rate_limiter);
                } else {
                  /*
                  std::stringstream workload;
                  //workload << "mp=" << multi_part_percentage << ",mh=" << multi_home_percentage << ",hot=" << 10000 << ",hot_records=" << 2 << ",rw=" << 100;
                  workload << "mh=" << multi_home_percentage << ",zipf=" << 0 << ",fi=0,mix=50_0_50";
                  auto special_configs = workload.str();
                  auto workload_type = make_unique<slog::YCSBWorkload>(config, reg, rep, "", special_configs, zipf_dist_vect);
                  */
                  std::stringstream workload;
                  //workload << "mp=" << multi_part_percentage << ",mh=" << multi_home_percentage << ",hot=" << 10000 << ",hot_records=" << 2 << ",rw=" << 100;
                  workload << "mh=" << 5 << ",mp=" << 0 << ",hot=10,rw=100";
                  auto special_configs = workload.str();
                  auto workload_type = make_unique<slog::SkewedWorkload>(config, reg, rep, "", special_configs);

                  tiga.AddTxnGenerator(move(workload_type), 0, clients_per_txn_gen, 60, rate_limiter);
                }
            }
          }
        }
      }
    }


    for (auto& [_, tiga] : test_tiga_) {
      tiga.StartTigaInNewThreads();
    }

    for (auto& [_, tiga] : test_tiga_) {
      tiga.StartTxnGeneratorInNewThreads();
    }
  }

  std::map<int, std::array<size_t, 4>> TxnGeneratorProgress(MachineId id) {
    auto it = test_tiga_.find(id);
    CHECK(it != test_tiga_.end());
    return it->second.TxnGeneratorProgress();
  }


  bool TxnGeneratorRunning(MachineId id) {
    auto it = test_tiga_.find(id);
    CHECK(it != test_tiga_.end());
    return it->second.TxnGeneratorRunning();
  }

 private:
  unordered_map<MachineId, TestTiga> test_tiga_;
};

TEST_P(E2ETxnGeneratorTigaTest, MultiClientDeployment) {
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
            if (p == 0) {
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

INSTANTIATE_TEST_SUITE_P(AllE2ETests, E2ETxnGeneratorTigaTest,
                         testing::Combine(testing::Values(false), testing::Values(1), testing::Values(0),
                                          testing::Values(10),  testing::Values(1)),
                         [](const testing::TestParamInfo<E2ETxnGeneratorTigaTest::ParamType>& info) {
                           bool local_sync_rep = std::get<0>(info.param);
                           auto num_clients = std::get<1>(info.param);

                           std::stringstream out;
                           out << (local_sync_rep ? "Sync" : "Async") << "_";
                           out << "Clients_" << num_clients << "_";
                           out << "workload_mp_" << std::get<2>(info.param) << "mh_" << std::get<3>(info.param) << "_";
                           out << "generators_" << std::get<4>(info.param) << "_";

                           return out.str();
                         });


int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);

  //FLAGS_v = 3;
  google::InstallFailureSignalHandler();
  return RUN_ALL_TESTS();
}
