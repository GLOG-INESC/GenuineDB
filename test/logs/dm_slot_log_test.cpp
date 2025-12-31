//
// Created by jrsoares on 23-06-2025.
//

#include <gtest/gtest.h>

#include <vector>

#include "common/log/deterministic_merge_log.h"
#include "common/proto_utils.h"
#include "test/test_utils.h"

using namespace std;
using namespace slog;
using internal::Envelope;

/**
 *  MultiSlotId Log Test
 * */

/**
 * Test Cases:
 *  - Epoch -> Advance
 *  - Advance -> Epoch
 *  - Client Removal
 * */

Batch* MakeBatch(BatchId batch_id, const vector<Transaction*>& txns, TransactionType batch_type) {
  Batch* batch = new Batch();
  batch->set_id(batch_id);
  batch->set_transaction_type(batch_type);
  for (auto txn : txns) {
    batch->mutable_transactions()->AddAllocated(txn);
  }
  return batch;
}

void AddClientSlots(slog::internal::ZiplogRegionSlots* epoch, ClientId client_id, int num_slots,
                    RegionId client_region) {
  auto client_slots = epoch->mutable_slots()->Add();
  client_slots->set_client_id(client_id);
  client_slots->set_num_slots(num_slots);
  client_slots->set_client_region(client_region);
  client_slots->set_client_type(MULTI_HOME_OR_LOCK_ONLY);
}

class DMSlotLogTest : public ::testing::Test {
 protected:
  void SetUp() {
    auto custom_config = internal::Configuration();
    custom_config.set_ddr_interval(30);

    configs_2reg_ = MakeTestConfigurations("asd", 2, 1, 1, custom_config);
    configs_3reg_ = MakeTestConfigurations("asd", 3, 1, 1, custom_config);
  }

  void CheckEntry(const Entry& entry, uint64_t deadline, ClientId client_id, uint64_t gsn, bool skip = false) {
    ASSERT_EQ(entry.skip, skip);
    ASSERT_EQ(entry.client_id, client_id);
    ASSERT_EQ(entry.gsn, gsn);
  }

  // Quick latency map:

  // - | 0  | 1 | 2 |
  // 0 | -  | 5 | 10|
  // 1 | 10 | - | 5 |
  // 2 | 10 | 5 | - |
  std::map<slog::RegionId, std::array<double, MAX_NUM_REGIONS>> region_latencies = {
      {0, {0, 5, 15}}, {1, {10, 0, 5}}, {2, {20, 5, 0}}};

  ConfigVec configs_2reg_;
  ConfigVec configs_3reg_;
};

TEST_F(DMSlotLogTest, OneClientEpochTest) {
  DeterministicMergeLog log("test", 1, milliseconds(100));

  // Create new Epoch
  auto epoch = new slog::internal::ZiplogRegionSlots();
  epoch->set_num_clients(1);
  epoch->set_gsn_base(0);

  AddClientSlots(epoch, 1, 2, 1);

  log.AddEpoch(epoch, 0);

  // Check allocated slot order
  ASSERT_EQ(log.GetNumEntries(), 2);

  Blocks* log_structure = log.GetLog();
  auto block_list = log_structure[0];
  auto block = block_list.begin();
  {
    auto entry = block.operator*()->at(0);

    // Cast Deadline to millisecond
    ASSERT_FALSE(entry.skip);
    ASSERT_EQ(entry.client_id, 1);
    ASSERT_EQ(entry.gsn, 0);
  }

  {
    auto entry = block.operator*()->at(1);

    // Cast Deadline to millisecond
    ASSERT_FALSE(entry.skip);
    ASSERT_EQ(entry.client_id, 1);
    ASSERT_EQ(entry.gsn, 1);
  }

}

TEST_F(DMSlotLogTest, TwoClientEpochTest) {
  DeterministicMergeLog log("test", 1, milliseconds(100));

  // Create new Epoch
  auto epoch = new slog::internal::ZiplogRegionSlots();
  epoch->set_num_clients(2);
  epoch->set_gsn_base(0);

  AddClientSlots(epoch, 1, 2, 1);
  AddClientSlots(epoch, 2, 2, 1);

  log.AddEpoch(epoch, 0);

  // Check allocated slot order
  ASSERT_EQ(log.GetNumEntries(), 4);

  Blocks* log_structure = log.GetLog();

  auto block_list = log_structure[0];
  auto block = block_list.begin();

  for (int i = 0; i < 4; i++){
    auto entry = block.operator*()->at(i);

    auto client_id = i % 2 == 0 ? 2 : 1;

    ASSERT_FALSE(entry.skip);
    ASSERT_EQ(entry.client_id, client_id);
    ASSERT_EQ(entry.gsn, i);
  }

}

TEST_F(DMSlotLogTest, TwoClientDifferentRegionsEpochTest) {
  DeterministicMergeLog log("test", 1, milliseconds(100));

  // Create new Epoch
  auto epoch = new slog::internal::ZiplogRegionSlots();
  epoch->set_num_clients(2);
  epoch->set_gsn_base(0);

  AddClientSlots(epoch, 1, 2, 0);
  AddClientSlots(epoch, 2, 2, 1);

  log.AddEpoch(epoch, 0);

  // Check allocated slot order
  ASSERT_EQ(log.GetNumEntries(), 4);

  Blocks* log_structure = log.GetLog();

  auto block_list = log_structure[0];
  auto block = block_list.begin();

  for (int i = 0; i < 4; i++){
    auto entry = block.operator*()->at(i);

    auto client_id = i % 2 == 0 ? 2 : 1;

    ASSERT_FALSE(entry.skip);
    ASSERT_EQ(entry.client_id, client_id);
    ASSERT_EQ(entry.gsn, i);
  }
}

TEST_F(DMSlotLogTest, TwoClientUnevenSlotsEpochTest) {
  DeterministicMergeLog log("test", 1, milliseconds(100));

  // Create new Epoch
  auto epoch = new slog::internal::ZiplogRegionSlots();
  epoch->set_num_clients(2);
  epoch->set_gsn_base(0);

  AddClientSlots(epoch, 1, 2, 1);
  AddClientSlots(epoch, 2, 3, 1);

  log.AddEpoch(epoch, 0);

  // Check allocated slot order
  ASSERT_EQ(log.GetNumEntries(), 5);

  Blocks* log_structure = log.GetLog();

  auto block_list = log_structure[0];
  auto block = block_list.begin();

  auto client_2_slots = std::set({0, 2, 3});
  for (int i = 0; i < 5; i++){
    auto entry = block.operator*()->at(i);

    auto client_id = client_2_slots.count(i) == 1 ? 2 : 1;

    ASSERT_FALSE(entry.skip);
    ASSERT_EQ(entry.client_id, client_id);
    ASSERT_EQ(entry.gsn, i);
  }
}


TEST_F(DMSlotLogTest, SimpleInsertion) {
  DeterministicMergeLog log_r0("R0 ", 0, milliseconds(100));

  // Create new Epoch
  auto epoch = new slog::internal::ZiplogRegionSlots();
  epoch->set_num_clients(1);
  epoch->set_gsn_base(0);

  AddClientSlots(epoch, 1, 2, 0);

  log_r0.AddEpoch(epoch, 0);

  // Create new batch
  auto batch_info = new slog::internal::ForwardZiplogBatch();
  batch_info->set_generator(1);
  batch_info->set_slots(1);

  // Make Txn that touched only on the remote log
  auto expected_txn_1 = MakeTransaction({{"A"}, {"B", KeyType::WRITE}});
  expected_txn_1->mutable_internal()->clear_involved_regions();
  expected_txn_1->mutable_internal()->add_involved_regions(0);
  expected_txn_1->mutable_internal()->add_involved_regions(1);

  auto batch = MakeBatch(1, {expected_txn_1}, MULTI_HOME_OR_LOCK_ONLY);
  batch_info->mutable_batch_data()->AddAllocated(batch);

  // Add batch to log
  log_r0.AddBatch(batch_info);

  // Transaction must have been inserted in the first slot
  Blocks* log_structure = log_r0.GetLog();

  auto block_list = log_structure[0];
  auto block = block_list.begin();

  CheckEntry(block.operator*()->at(0), 55, 1, 0);
  auto result_batch = block.operator*()->at(0).batch;

  ASSERT_NE(result_batch, nullptr);

  ASSERT_EQ(1, result_batch->transactions_size());
  ASSERT_EQ(1000, result_batch->transactions(0).internal().id());
}

TEST_F(DMSlotLogTest, SimpleInsertionBatch) {
  DeterministicMergeLog log_r0("R0 ", 0, milliseconds(100));

  // Create new Epoch
  auto epoch = new slog::internal::ZiplogRegionSlots();
  epoch->set_num_clients(1);
  epoch->set_gsn_base(0);

  AddClientSlots(epoch, 1, 2, 0);

  log_r0.AddEpoch(epoch, 0);

  // Create new batch
  auto batch_info = new slog::internal::ForwardZiplogBatch();
  batch_info->set_generator(1);
  batch_info->set_slots(1);

  // Make Txn that touched only on the remote log
  auto expected_txn_1 = MakeTransaction({{"A"}, {"B", KeyType::WRITE}});
  expected_txn_1->mutable_internal()->clear_involved_regions();
  expected_txn_1->mutable_internal()->add_involved_regions(0);
  expected_txn_1->mutable_internal()->add_involved_regions(1);

  auto expected_txn_2 = MakeTransaction({{"A"}, {"B", KeyType::WRITE}});
  expected_txn_2->mutable_internal()->clear_involved_regions();
  expected_txn_2->mutable_internal()->add_involved_regions(0);
  expected_txn_2->mutable_internal()->add_involved_regions(1);
  expected_txn_2->mutable_internal()->set_id(2);

  auto batch = MakeBatch(1, {expected_txn_1, expected_txn_2}, MULTI_HOME_OR_LOCK_ONLY);
  batch_info->mutable_batch_data()->AddAllocated(batch);

  // Add batch to log
  log_r0.AddBatch(batch_info);

  // Transaction must have been inserted in the first slot
  Blocks* log_structure = log_r0.GetLog();

  auto block_list = log_structure[0];
  auto block = block_list.begin();

  CheckEntry(block.operator*()->at(0), 55, 1, 0);
  auto result_batch = block.operator*()->at(0).batch;

  ASSERT_NE(result_batch, nullptr);

  ASSERT_EQ(2, result_batch->transactions_size());
  ASSERT_EQ(1000, result_batch->transactions(0).internal().id());
  ASSERT_EQ(2, result_batch->transactions(1).internal().id());
}

// Client 0 touches on region 0 and 1
// Transaction should be inserted correctly in region 0 and 1, and skip on region 2
TEST_F(DMSlotLogTest, MultiSlotInsertionMultiRegionSkip) {
  DeterministicMergeLog log_r0("R0 ", 0, milliseconds(100));
  DeterministicMergeLog log_r1("R1 ", 1, milliseconds(100));
  DeterministicMergeLog log_r2("R2 ", 2, milliseconds(100));

  // Create new Epoch
  auto epoch = new slog::internal::ZiplogRegionSlots();
  epoch->set_num_clients(1);
  epoch->set_gsn_base(0);

  AddClientSlots(epoch, 2, 2, 0);

  log_r0.AddEpoch(epoch, 0);
  log_r1.AddEpoch(epoch, 0);
  log_r2.AddEpoch(epoch, 0);

  // Create new batch

  auto logs = {&log_r0, &log_r1, &log_r2};
  for (auto log : logs) {
    auto batch_info = new slog::internal::ForwardZiplogBatch();
    batch_info->set_generator(2);
    batch_info->set_slots(1);

    // Make Txn that touched only on the remote log
    auto expected_txn_1 = MakeTransaction({{"A"}, {"B", KeyType::WRITE}});
    expected_txn_1->mutable_internal()->clear_involved_regions();
    expected_txn_1->mutable_internal()->add_involved_regions(0);
    expected_txn_1->mutable_internal()->add_involved_regions(1);

    auto batch = MakeBatch(1, {expected_txn_1}, MULTI_HOME_OR_LOCK_ONLY);
    batch_info->mutable_batch_data()->AddAllocated(batch);

    // Add batch to log
    log->AddBatch(batch_info);
  }

  {
    // Transaction must have been inserted in the first slot
    Blocks* log_structure = log_r0.GetLog();

    auto block_list = log_structure[0];
    auto block = block_list.begin();

    CheckEntry(block.operator*()->at(0), 65, 2, 0);
    auto result_batch = block.operator*()->at(0).batch;

    ASSERT_NE(result_batch, nullptr);

    ASSERT_EQ(1, result_batch->transactions_size());
    ASSERT_EQ(1000, result_batch->transactions(0).internal().id());
  }

  {
    // Transaction must have been inserted in the first slot
    Blocks* log_structure = log_r1.GetLog();

    auto block_list = log_structure[0];
    auto block = block_list.begin();

    CheckEntry(block.operator*()->at(0), 0, 2, 0);
    auto result_batch = block.operator*()->at(0).batch;

    ASSERT_NE(result_batch, nullptr);

    ASSERT_EQ(1, result_batch->transactions_size());
    ASSERT_EQ(1000, result_batch->transactions(0).internal().id());
  }

  {
    // Transaction must have been inserted in the first slot
    Blocks* log_structure = log_r2.GetLog();

    auto block_list = log_structure[0];
    auto block = block_list.begin();

    CheckEntry(block.operator*()->at(0), 0, 2, 0, true);
    ASSERT_EQ(block.operator*()->at(0).batch, nullptr);
  }
}

// Client 0 touches on region 0 and 2
// Transaction should be inserted correctly in all regions
TEST_F(DMSlotLogTest, MultiSlotInsertionInsertAll) {
  DeterministicMergeLog log_r0("R0 ", 0, milliseconds(100));
  DeterministicMergeLog log_r1("R1 ", 1, milliseconds(100));
  DeterministicMergeLog log_r2("R2 ", 2, milliseconds(100));

  // Create new Epoch
  auto epoch = new slog::internal::ZiplogRegionSlots();
  epoch->set_num_clients(1);
  epoch->set_gsn_base(0);

  AddClientSlots(epoch, 2, 2, 0);

  log_r0.AddEpoch(epoch, 0);
  log_r1.AddEpoch(epoch, 0);
  log_r2.AddEpoch(epoch, 0);

  auto logs = {&log_r0, &log_r1, &log_r2};
  for (auto log : logs) {
    auto batch_info = new slog::internal::ForwardZiplogBatch();
    batch_info->set_generator(2);
    batch_info->set_slots(1);

    // Make Txn that touched only on the remote log
    auto expected_txn_1 = MakeTransaction({{"A"}, {"B", KeyType::WRITE}});
    expected_txn_1->mutable_internal()->clear_involved_regions();
    expected_txn_1->mutable_internal()->add_involved_regions(0);
    expected_txn_1->mutable_internal()->add_involved_regions(1);
    expected_txn_1->mutable_internal()->add_involved_regions(2);

    auto batch = MakeBatch(1, {expected_txn_1}, MULTI_HOME_OR_LOCK_ONLY);
    batch_info->mutable_batch_data()->AddAllocated(batch);

    // Add batch to log
    log->AddBatch(batch_info);

    // Transaction must have been inserted in the first slot
    Blocks* log_structure = log->GetLog();

    auto block_list = log_structure[0];
    auto block = block_list.begin();

    CheckEntry(block.operator*()->at(0), 65, 2, 0);
    auto result_batch = block.operator*()->at(0).batch;

    ASSERT_NE(result_batch, nullptr);

    ASSERT_EQ(1, result_batch->transactions_size());
    ASSERT_EQ(1000, result_batch->transactions(0).internal().id());

  }
}

// Log Processing Tests

TEST_F(DMSlotLogTest, OutOfOrderInsertion) {
  DeterministicMergeLog log("test", 1, milliseconds(100));

  // Create new Epoch
  auto epoch = new slog::internal::ZiplogRegionSlots();
  epoch->set_num_clients(2);
  epoch->set_gsn_base(0);

  AddClientSlots(epoch, 1, 2, 1);
  AddClientSlots(epoch, 2, 2, 2);

  log.AddEpoch(epoch, 0);

  {
    // Create new batch
    auto batch_info = new slog::internal::ForwardZiplogBatch();
    batch_info->set_generator(1);
    batch_info->set_slots(1);

    // Make Txn that touched only on the remote log
    auto expected_txn_1 = MakeTransaction({{"A"}, {"B", KeyType::WRITE}});
    expected_txn_1->mutable_internal()->clear_involved_regions();
    expected_txn_1->mutable_internal()->add_involved_regions(1);
    expected_txn_1->mutable_internal()->add_involved_regions(2);
    expected_txn_1->mutable_internal()->set_id(1);

    auto batch = MakeBatch(1, {expected_txn_1}, MULTI_HOME_OR_LOCK_ONLY);
    batch_info->mutable_batch_data()->AddAllocated(batch);

    // Add batch to log
    log.AddBatch(batch_info);
  }

  ASSERT_FALSE(log.HasNextBatch());

  {
    // Create new batch
    auto batch_info = new slog::internal::ForwardZiplogBatch();
    batch_info->set_generator(2);
    batch_info->set_slots(1);

    // Make Txn that touched only on the remote log
    auto expected_txn_1 = MakeTransaction({{"A"}, {"B", KeyType::WRITE}});
    expected_txn_1->mutable_internal()->clear_involved_regions();
    expected_txn_1->mutable_internal()->add_involved_regions(0);
    expected_txn_1->mutable_internal()->add_involved_regions(1);
    expected_txn_1->mutable_internal()->set_id(2);

    auto batch = MakeBatch(2, {expected_txn_1}, MULTI_HOME_OR_LOCK_ONLY);
    batch_info->mutable_batch_data()->AddAllocated(batch);

    // Add batch to log
    log.AddBatch(batch_info);
  }

  ASSERT_TRUE(log.HasNextBatch());

  {
    auto next_batch_tup = log.NextBatch();

    ASSERT_EQ(std::get<0>(next_batch_tup), 0);
    ASSERT_FALSE(std::get<2>(next_batch_tup));
    auto next_batch = std::get<1>(next_batch_tup);

    ASSERT_EQ(1, next_batch->transactions_size());
    ASSERT_EQ(2, next_batch->transactions(0).internal().id());
  }

  ASSERT_TRUE(log.HasNextBatch());

  {
    auto next_batch_tup = log.NextBatch();

    ASSERT_EQ(std::get<0>(next_batch_tup), 1);
    ASSERT_FALSE(std::get<2>(next_batch_tup));
    auto next_batch = std::get<1>(next_batch_tup);

    ASSERT_EQ(1, next_batch->transactions_size());
    ASSERT_EQ(1, next_batch->transactions(0).internal().id());
  }
}

TEST_F(DMSlotLogTest, EmptyBatch) {
  DeterministicMergeLog log_r0("R0 ", 0, milliseconds(100));

  // Create new Epoch
  auto epoch = new slog::internal::ZiplogRegionSlots();
  epoch->set_num_clients(1);
  epoch->set_gsn_base(0);

  AddClientSlots(epoch, 1, 2, 0);

  log_r0.AddEpoch(epoch, 0);

  // Create new batch
  auto batch_info = new slog::internal::ForwardZiplogBatch();
  batch_info->set_generator(1);
  batch_info->set_slots(1);

  // Make Txn that touched only on the remote log
  auto expected_txn_1 = MakeTransaction({{"A"}, {"B", KeyType::WRITE}});
  expected_txn_1->mutable_internal()->clear_involved_regions();
  expected_txn_1->mutable_internal()->set_noop(true);

  auto batch = MakeBatch(1, {expected_txn_1}, MULTI_HOME_OR_LOCK_ONLY);
  batch_info->mutable_batch_data()->AddAllocated(batch);

  // Add batch to log
  log_r0.AddBatch(batch_info);

  ASSERT_TRUE(log_r0.HasNextBatch());

  {
    auto next_batch_tup = log_r0.NextBatch();

    ASSERT_EQ(std::get<0>(next_batch_tup), 0);
    ASSERT_TRUE(std::get<2>(next_batch_tup));
    ASSERT_EQ(std::get<1>(next_batch_tup), nullptr);
  }
}

TEST_F(DMSlotLogTest, SkipBatchWithThx) {
  DeterministicMergeLog log_r0("R0 ", 0, milliseconds(100));

  // Create new Epoch
  auto epoch = new slog::internal::ZiplogRegionSlots();
  epoch->set_num_clients(1);
  epoch->set_gsn_base(0);

  AddClientSlots(epoch, 1, 4, 0);

  log_r0.AddEpoch(epoch, 0);

  {
    // Create new batch
    auto batch_info = new slog::internal::ForwardZiplogBatch();
    batch_info->set_generator(1);
    batch_info->set_slots(2);

    // Make Txn that touched only on the remote log
    auto expected_txn_1 = MakeTransaction({{"A"}, {"B", KeyType::WRITE}});
    expected_txn_1->mutable_internal()->clear_involved_regions();
    expected_txn_1->mutable_internal()->add_involved_regions(0);
    expected_txn_1->mutable_internal()->add_involved_regions(1);
    expected_txn_1->mutable_internal()->set_id(2);

    auto batch = MakeBatch(2, {expected_txn_1}, MULTI_HOME_OR_LOCK_ONLY);
    batch_info->mutable_batch_data()->AddAllocated(batch);

    // Add batch to log
    log_r0.AddBatch(batch_info);
  }

  ASSERT_TRUE(log_r0.HasNextBatch());

  {
    auto next_batch_tup = log_r0.NextBatch();

    ASSERT_EQ(std::get<0>(next_batch_tup), 0);
    ASSERT_FALSE(std::get<2>(next_batch_tup));
    auto next_batch = std::get<1>(next_batch_tup);

    ASSERT_EQ(1, next_batch->transactions_size());
    ASSERT_EQ(2, next_batch->transactions(0).internal().id());
  }

  ASSERT_TRUE(log_r0.HasNextBatch());

  {
    auto next_batch_tup = log_r0.NextBatch();

    ASSERT_EQ(std::get<0>(next_batch_tup), 1);
    ASSERT_TRUE(std::get<2>(next_batch_tup));
    ASSERT_EQ(std::get<1>(next_batch_tup), nullptr);
  }

  ASSERT_FALSE(log_r0.HasNextBatch());
}

TEST_F(DMSlotLogTest, SkipBatchWithNoop) {
  DeterministicMergeLog log_r0("R0 ", 0, milliseconds(100));

  // Create new Epoch
  auto epoch = new slog::internal::ZiplogRegionSlots();
  epoch->set_num_clients(1);
  epoch->set_gsn_base(0);

  AddClientSlots(epoch, 1, 4, 0);

  log_r0.AddEpoch(epoch, 0);

  // Create new batch
  auto batch_info = new slog::internal::ForwardZiplogBatch();
  batch_info->set_generator(1);
  batch_info->set_slots(2);

  // Make Txn that touched only on the remote log
  auto expected_txn_1 = MakeTransaction({{"A"}, {"B", KeyType::WRITE}});
  expected_txn_1->mutable_internal()->clear_involved_regions();
  expected_txn_1->mutable_internal()->set_noop(true);

  auto batch = MakeBatch(1, {expected_txn_1}, MULTI_HOME_OR_LOCK_ONLY);

  batch_info->mutable_batch_data()->AddAllocated(batch);

  // Add batch to log
  log_r0.AddBatch(batch_info);

  for (int i = 0; i < 2; i++) {
    ASSERT_TRUE(log_r0.HasNextBatch());

    {
      auto next_batch_tup = log_r0.NextBatch();

      ASSERT_EQ(std::get<0>(next_batch_tup), i);
      ASSERT_TRUE(std::get<2>(next_batch_tup));
      ASSERT_EQ(std::get<1>(next_batch_tup), nullptr);
    }
  }

  ASSERT_FALSE(log_r0.HasNextBatch());
}
/*


TEST_F(MultiSlotLogTest, ClientRemoval) {

  MultiSlotLog log("test", 1, milliseconds(100));

  // Create new Epoch
  auto epoch = new slog::internal::ZiplogRegionSlots();
  epoch->set_num_clients(2);
  epoch->set_gsn_base(0);

  AddClientSlots(epoch, 1, 2, 1);
  AddClientSlots(epoch, 2, 2, 1);

  log.AddEpoch(epoch, 0);

  {
    // Create new batch
    auto batch_info = new slog::internal::ForwardZiplogBatch();
    batch_info->set_generator(1);
    batch_info->set_slots(1);

    // Make Txn that touched only on the remote log
    auto expected_txn_1 = MakeTransaction({{"A"}, {"B", KeyType::WRITE}});
    expected_txn_1->mutable_internal()->clear_involved_regions();
    expected_txn_1->mutable_internal()->add_involved_regions(0);
    expected_txn_1->mutable_internal()->add_involved_regions(1);

    auto batch = MakeBatch(1, {expected_txn_1}, MULTI_HOME_OR_LOCK_ONLY);
    batch_info->mutable_batch_data()->AddAllocated(batch);

    // Add batch to log
    log.AddBatch(batch_info);
  }

  ASSERT_FALSE(log.HasNextBatch());

  log.RemoveClient(2);

  ASSERT_TRUE(log.HasNextBatch());

  {
    auto next_batch_tup = log.NextBatch();

    ASSERT_EQ(std::get<0>(next_batch_tup), 0);
    ASSERT_TRUE(std::get<2>(next_batch_tup));
    ASSERT_EQ(std::get<1>(next_batch_tup), nullptr);
  }

  ASSERT_TRUE(log.HasNextBatch());

  {
    auto next_batch_tup = log.NextBatch();

    ASSERT_EQ(std::get<0>(next_batch_tup), 1);
    ASSERT_FALSE(std::get<2>(next_batch_tup));
    auto next_batch = std::get<1>(next_batch_tup);

    ASSERT_EQ(1, next_batch->transactions_size());
    ASSERT_EQ(1000, next_batch->transactions(0).internal().id());
  }

}



*/
