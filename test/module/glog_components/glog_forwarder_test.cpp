
#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/proto_utils.h"
#include "connection/broker.h"
#include "module/server.h"
#include "proto/api.pb.h"
#include "storage/mem_only_storage.h"
#include "test/test_utils.h"
#include "test/glog_test_utils.h"

using namespace std;
using namespace slog;
using namespace glog;

class GlogForwarderTest : public ::testing::Test {
 protected:
  static const size_t NUM_MACHINES = 4;

  void SetUp() {

    auto custom_config = internal::Configuration();
    custom_config.set_bypass_mh_orderer(false);
    custom_config.set_synchronized_batching(false);
    custom_config.mutable_simple_partitioning()->set_num_records(2500000);
    custom_config.mutable_simple_partitioning()->set_record_size_bytes(100);

    configs = MakeGlogTestConfigurations("glog_forwarder", 2 /* num_regions */, 1 /* num_replicas */,
                                         2 /* num_partitions */, custom_config);

    for (size_t i = 0; i < NUM_MACHINES; i++) {
      test_slogs[i] = make_unique<TestGlog>(configs[i]);
      test_slogs[i]->AddServerAndClient();
      test_slogs[i]->AddSimpleMetadataForwarder();
      test_slogs[i]->AddOutputSocket(kSequencerChannel);
      test_slogs[i]->AddOutputSocket(kMultiHomeOrdererChannel);
    }


    // Region 0
    test_slogs[0]->Data("0", {"xxxxx", 0, 0});
    test_slogs[0]->Data("2", {"xxxxx", 1, 0});
    test_slogs[1]->Data("1", {"xxxxx", 0, 0});
    test_slogs[1]->Data("3", {"xxxxx", 1, 0});
    // Region 1
    test_slogs[2]->Data("0", {"xxxxx", 0, 0});
    test_slogs[2]->Data("2", {"xxxxx", 1, 0});
    test_slogs[3]->Data("1", {"xxxxx", 0, 0});
    test_slogs[3]->Data("3", {"xxxxx", 1, 0});

    for (const auto& test_slog : test_slogs) {
      test_slog->StartGlogInNewThreads();
    }
  }

  Transaction* ReceiveOnSequencerChannel(vector<size_t> machines) {
    CHECK(!machines.empty());
    // machine, inproc, pollitem
    std::vector<std::pair<size_t, bool>> info;
    std::vector<zmq::pollitem_t> poll_items;
    for (auto m : machines) {
      info.emplace_back(m, true);
      poll_items.push_back(test_slogs[m]->GetPollItemForOutputSocket(kSequencerChannel, true));
      info.emplace_back(m, false);
      poll_items.push_back(test_slogs[m]->GetPollItemForOutputSocket(kSequencerChannel, false));
    }
    auto rc = zmq::poll(poll_items);
    if (rc <= 0) return nullptr;
    for (size_t i = 0; i < poll_items.size(); i++) {
      if (poll_items[i].revents & ZMQ_POLLIN) {
        auto [m, inproc] = info[i];
        auto req_env = test_slogs[m]->ReceiveFromOutputSocket(kSequencerChannel, inproc);
        if (req_env == nullptr) {
          return nullptr;
        }
        return ExtractTxn(req_env);
      }
    }

    return nullptr;
  }

  Transaction* ReceiveOnOrdererChannel(size_t machine) {
    auto req_env = test_slogs[machine]->ReceiveFromOutputSocket(kMultiHomeOrdererChannel);
    if (req_env == nullptr) {
      return nullptr;
    }
    return ExtractTxn(req_env);
  }

  unique_ptr<TestGlog> test_slogs[NUM_MACHINES];
  ConfigVec configs;

 private:
  Transaction* ExtractTxn(EnvelopePtr& req) {
    if (req->request().type_case() != internal::Request::kForwardTxn) {
      return nullptr;
    }
    return req->mutable_request()->mutable_forward_txn()->release_txn();
  }
};

TEST_F(GlogForwarderTest, GlogForwardToSameRegion) {

  // This txn needs to lookup from both partitions in a region
  auto txn = MakeTransaction({{"0"}, {"1", KeyType::WRITE}});
  // Send to partition 0 of region 0
  test_slogs[0]->SendTxn(txn);
  auto forwarded_txn = ReceiveOnSequencerChannel({0});

  // The txn should be forwarded to the sequencer of the same machine
  ASSERT_TRUE(forwarded_txn != nullptr);
  ASSERT_EQ(TransactionType::SINGLE_HOME, forwarded_txn->internal().type());
  ASSERT_EQ(0, forwarded_txn->internal().home());
  ASSERT_EQ(0U, TxnValueEntry(*forwarded_txn, "0").metadata().master());
  ASSERT_EQ(0U, TxnValueEntry(*forwarded_txn, "0").metadata().counter());
  ASSERT_EQ(0U, TxnValueEntry(*forwarded_txn, "1").metadata().master());
  ASSERT_EQ(0U, TxnValueEntry(*forwarded_txn, "1").metadata().counter());

  // Validate involved
  {
    // Assert correct active and involved
    auto participating_partitions = forwarded_txn->internal().involved_partitions();

    ASSERT_EQ(2, participating_partitions.size());
    std::set<MachineId> expected_participants;
    expected_participants.insert(MakeMachineId(0,0,0));
    expected_participants.insert(MakeMachineId(0,0,1));


    for (auto & participant : participating_partitions){
      auto x = expected_participants.find(participant);

      ASSERT_TRUE(x != expected_participants.end());
      expected_participants.erase(participant);
    }
  }

  // Validate participating partitions

  {
    auto active_partitions = forwarded_txn->internal().active_partitions();

    ASSERT_EQ(1, active_partitions.size());

    std::set<MachineId> expected_active_participants;
    expected_active_participants.insert(MakeMachineId(0,0,1));

    for (auto & participant : active_partitions){
      auto x = expected_active_participants.find(participant);

      ASSERT_TRUE(x != expected_active_participants.end());
      expected_active_participants.erase(participant);
    }
  }

  // Validate Regions
  {
    auto involved_regions = forwarded_txn->internal().involved_regions();
    ASSERT_EQ(1, involved_regions.size());

    ASSERT_EQ(involved_regions[0], 0);
  }

}

TEST_F(GlogForwarderTest, GlogForwardToAnotherRegion) {
  // Send to partition 0 and 1 of Region 2
  // It must go to the STUB
  test_slogs[1]->SendTxn(MakeTransaction({{"2"}, {"3", KeyType::WRITE}}));

  // Send to partition 0 of region 1.
  // This also needs to go to the stub
  //test_slogs[2]->SendTxn(MakeTransaction({{"0"}}));


  {
    auto forwarded_txn = ReceiveOnOrdererChannel(1);
    // A txn should be forwarded to one of the two schedulers in
    // region 1
    ASSERT_TRUE(forwarded_txn != nullptr);
    ASSERT_EQ(TransactionType::SINGLE_HOME, forwarded_txn->internal().type());
    ASSERT_EQ(1, forwarded_txn->internal().home());
    ASSERT_EQ(1U, TxnValueEntry(*forwarded_txn, "2").metadata().master());
    ASSERT_EQ(0U, TxnValueEntry(*forwarded_txn, "2").metadata().counter());
    ASSERT_EQ(1U, TxnValueEntry(*forwarded_txn, "3").metadata().master());
    ASSERT_EQ(0U, TxnValueEntry(*forwarded_txn, "3").metadata().counter());

    // Assert correct active and involved
    auto participating_partitions = forwarded_txn->internal().involved_partitions();

    ASSERT_EQ(2, participating_partitions.size());
    std::set<MachineId> expected_participants;
    expected_participants.insert(MakeMachineId(1,0,0));
    expected_participants.insert(MakeMachineId(1,0,1));


    for (auto & participant : participating_partitions){
      auto x = expected_participants.find(participant);

      ASSERT_TRUE(x != expected_participants.end());
      expected_participants.erase(participant);
    }

    auto active_partitions = forwarded_txn->internal().active_partitions();

    ASSERT_EQ(1, active_partitions.size());

    std::set<MachineId> expected_active_participants;
    expected_active_participants.insert(MakeMachineId(1,0,1));

    for (auto & participant : active_partitions){
      auto x = expected_active_participants.find(participant);

      ASSERT_TRUE(x != expected_active_participants.end());
      expected_active_participants.erase(participant);
    }

    {
      auto involved_regions = forwarded_txn->internal().involved_regions();
      ASSERT_EQ(1, involved_regions.size());

      ASSERT_EQ(involved_regions[0], 1);
    }


  }
}

TEST_F(GlogForwarderTest, GlogTransactionHasNewKeys) {
  // This txn needs to lookup from both partitions in a region
  auto txn = MakeTransaction({{"7"}, {"6", KeyType::WRITE}});
  // Send to partition 0 of region 0
  test_slogs[3]->SendTxn(txn);

  auto forwarded_txn = ReceiveOnSequencerChannel({0, 1, 2, 3});
  // The txn should be forwarded to the scheduler of the same machine
  auto metadata_initializer = test_slogs[3]->metadata_initializer();
  ASSERT_TRUE(forwarded_txn != nullptr);
  ASSERT_EQ(TransactionType::SINGLE_HOME, forwarded_txn->internal().type());
  auto metadata1 = metadata_initializer->Compute("6");
  ASSERT_EQ(metadata1.master, TxnValueEntry(*forwarded_txn, "6").metadata().master());
  ASSERT_EQ(metadata1.counter, TxnValueEntry(*forwarded_txn, "6").metadata().counter());
  auto metadata2 = metadata_initializer->Compute("7");
  ASSERT_EQ(metadata2.master, TxnValueEntry(*forwarded_txn, "7").metadata().master());
  ASSERT_EQ(metadata2.counter, TxnValueEntry(*forwarded_txn, "7").metadata().counter());

  // Assert correct active and involved
  auto participating_partitions = forwarded_txn->internal().involved_partitions();

  ASSERT_EQ(2, participating_partitions.size());
  std::set<MachineId> expected_participants;
  expected_participants.insert(MakeMachineId(1,0,0));
  expected_participants.insert(MakeMachineId(1,0,1));


  for (auto & participant : participating_partitions){
    auto x = expected_participants.find(participant);

    ASSERT_TRUE(x != expected_participants.end());
    expected_participants.erase(participant);
  }

  auto active_partitions = forwarded_txn->internal().active_partitions();

  ASSERT_EQ(1, active_partitions.size());

  std::set<MachineId> expected_active_participants;
  expected_active_participants.insert(MakeMachineId(1,0,0));

  for (auto & participant : active_partitions){
    auto x = expected_active_participants.find(participant);

    ASSERT_TRUE(x != expected_active_participants.end());
    expected_active_participants.erase(participant);
  }

  {
    auto involved_regions = forwarded_txn->internal().involved_regions();
    ASSERT_EQ(1, involved_regions.size());

    ASSERT_EQ(involved_regions[0], 1);
  }

}

TEST_F(GlogForwarderTest, GlogForwardMultiHome) {
  // This txn involves data mastered by two regions
  auto txn = MakeTransaction({{"0"}, {"2", KeyType::WRITE}});

  test_slogs[1]->SendTxn(txn);
  auto forwarded_txn = ReceiveOnOrdererChannel(1);

  ASSERT_TRUE(forwarded_txn != nullptr);
  ASSERT_EQ(TransactionType::MULTI_HOME_OR_LOCK_ONLY, forwarded_txn->internal().type());
  ASSERT_EQ(-1, forwarded_txn->internal().home());
  ASSERT_EQ(0U, TxnValueEntry(*forwarded_txn, "0").metadata().master());
  ASSERT_EQ(0U, TxnValueEntry(*forwarded_txn, "0").metadata().counter());
  ASSERT_EQ(1U, TxnValueEntry(*forwarded_txn, "2").metadata().master());
  ASSERT_EQ(0U, TxnValueEntry(*forwarded_txn, "2").metadata().counter());

  {
    // Assert correct active and involved
    auto participating_partitions = forwarded_txn->internal().involved_partitions();

    ASSERT_EQ(2, participating_partitions.size());
    std::set<MachineId> expected_participants;
    expected_participants.insert(MakeMachineId(0,0,0));
    expected_participants.insert(MakeMachineId(1,0,0));


    for (auto & participant : participating_partitions){
      auto x = expected_participants.find(participant);

      ASSERT_TRUE(x != expected_participants.end());
      expected_participants.erase(participant);
    }
  }

  {
    auto active_partitions = forwarded_txn->internal().active_partitions();

    ASSERT_EQ(1, active_partitions.size());

    std::set<MachineId> expected_active_participants;
    expected_active_participants.insert(MakeMachineId(1,0,0));

    for (auto & participant : active_partitions){
      auto x = expected_active_participants.find(participant);

      ASSERT_TRUE(x != expected_active_participants.end());
      expected_active_participants.erase(participant);
    }
  }


  {
    auto involved_regions = forwarded_txn->internal().involved_regions();
    ASSERT_EQ(2, involved_regions.size());

    set<RegionId> expected_regions;
    expected_regions.insert(0);
    expected_regions.insert(1);

    for (auto & region : involved_regions){
      auto x = expected_regions.find(region);

      ASSERT_TRUE(x != expected_regions.end());
      expected_regions.erase(region);
    }
  }
}

TEST_F(GlogForwarderTest, GlogForwardMultiHomeMultiPartition) {
  // This txn involves data mastered by two regions
  auto txn = MakeTransaction({{"0"}, {"1", KeyType::WRITE}, {"2", KeyType::WRITE}, {"3"}});

  test_slogs[1]->SendTxn(txn);
  auto forwarded_txn = ReceiveOnOrdererChannel(1);

  ASSERT_TRUE(forwarded_txn != nullptr);
  ASSERT_EQ(TransactionType::MULTI_HOME_OR_LOCK_ONLY, forwarded_txn->internal().type());
  ASSERT_EQ(-1, forwarded_txn->internal().home());
  ASSERT_EQ(0U, TxnValueEntry(*forwarded_txn, "0").metadata().master());
  ASSERT_EQ(0U, TxnValueEntry(*forwarded_txn, "0").metadata().counter());
  ASSERT_EQ(1U, TxnValueEntry(*forwarded_txn, "2").metadata().master());
  ASSERT_EQ(0U, TxnValueEntry(*forwarded_txn, "2").metadata().counter());
  ASSERT_EQ(0U, TxnValueEntry(*forwarded_txn, "1").metadata().master());
  ASSERT_EQ(0U, TxnValueEntry(*forwarded_txn, "1").metadata().counter());
  ASSERT_EQ(1U, TxnValueEntry(*forwarded_txn, "3").metadata().master());
  ASSERT_EQ(0U, TxnValueEntry(*forwarded_txn, "3").metadata().counter());
  {
    // Assert correct active and involved
    auto participating_partitions = forwarded_txn->internal().involved_partitions();

    ASSERT_EQ(4, participating_partitions.size());
    std::set<MachineId> expected_participants;
    expected_participants.insert(MakeMachineId(0,0,0));
    expected_participants.insert(MakeMachineId(0,0,1));
    expected_participants.insert(MakeMachineId(1,0,0));
    expected_participants.insert(MakeMachineId(1,0,1));

    for (auto & participant : participating_partitions){
      auto x = expected_participants.find(participant);

      ASSERT_TRUE(x != expected_participants.end());
      expected_participants.erase(participant);
    }
  }

  {
    auto active_partitions = forwarded_txn->internal().active_partitions();

    ASSERT_EQ(2, active_partitions.size());

    std::set<MachineId> expected_active_participants;
    expected_active_participants.insert(MakeMachineId(1,0,0));
    expected_active_participants.insert(MakeMachineId(0,0,1));

    for (auto & participant : active_partitions){
      auto x = expected_active_participants.find(participant);

      ASSERT_TRUE(x != expected_active_participants.end());
      expected_active_participants.erase(participant);
    }
  }


  {
    auto involved_regions = forwarded_txn->internal().involved_regions();
    ASSERT_EQ(2, involved_regions.size());

    set<RegionId> expected_regions;
    expected_regions.insert(0);
    expected_regions.insert(1);

    for (auto & region : involved_regions){
      auto x = expected_regions.find(region);

      ASSERT_TRUE(x != expected_regions.end());
      expected_regions.erase(region);
    }
  }
}

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  google::InstallFailureSignalHandler();
  return RUN_ALL_TESTS();
}