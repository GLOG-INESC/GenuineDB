//
// Created by jrsoares on 23-06-2025.
//

#include "common/log/pessimistic_dm_log.h"

#include <gtest/gtest.h>

#include <vector>

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

class PessimisticSlotLogTest : public ::testing::Test {
 protected:

  void SetUp(){
    auto custom_config = internal::Configuration();
    custom_config.set_ddr_interval(30);

    configs_2reg_ = MakeTestConfigurations("asd", 2, 1, 1, custom_config);
    configs_3reg_ = MakeTestConfigurations("asd", 3, 1, 1, custom_config);

  }

  void CheckEntry(const Entry& entry, uint64_t deadline, ClientId client_id, uint64_t gsn, bool skip = false) {
    ASSERT_EQ(entry.deadline / 1000000, deadline);
    ASSERT_EQ(entry.skip, skip);
    ASSERT_EQ(entry.client_id, client_id);
    ASSERT_EQ(entry.gsn, gsn);
  }

  // Quick latency map:

  // - | 0  | 1 | 2 |
  // 0 | -  | 5 | 10|
  // 1 | 10 | - | 5 |
  // 2 | 10 | 5 | - |
  std::map<slog::RegionId, std::array<double, MAX_NUM_REGIONS>> region_latencies = {{0, {0, 5, 15}}, {1, {10, 0, 5}}, {2, {20, 5, 0}}};

  ConfigVec configs_2reg_;
  ConfigVec configs_3reg_;

};

TEST_F(PessimisticSlotLogTest, OneClientEpochTest) {


  PessimisticDMLog log("test", 1, milliseconds(100), configs_2reg_[0], region_latencies);

  // Create new Epoch
  auto epoch = new slog::internal::ZiplogRegionSlots();
  epoch->set_num_clients(1);
  epoch->set_gsn_base(0);

  AddClientSlots(epoch, 1, 2, 1);

  log.AddEpoch(epoch, 0);

  // Check allocated slot order
  ASSERT_EQ(log.GetNumEntries(), 1);

  Blocks* log_structure = log.GetLog();

  auto block_list = log_structure[0];
  auto block = block_list.begin();
  auto entry = block.operator*()->at(0);

  // Cast Deadline to millisecond
  ASSERT_EQ(entry.deadline / 1000000, 60);
  ASSERT_FALSE(entry.skip);
  ASSERT_EQ(entry.client_id, 1);
  ASSERT_EQ(entry.gsn, 0);
}

TEST_F(PessimisticSlotLogTest, TwoClientEpochTest) {
  PessimisticDMLog log("test", 1, milliseconds(100), configs_2reg_[0], region_latencies);

  // Create new Epoch
  auto epoch = new slog::internal::ZiplogRegionSlots();
  epoch->set_num_clients(2);
  epoch->set_gsn_base(0);

  AddClientSlots(epoch, 1, 2, 1);
  AddClientSlots(epoch, 2, 2, 1);

  log.AddEpoch(epoch, 0);

  // Check allocated slot order
  ASSERT_EQ(log.GetNumEntries(), 2);

  Blocks* log_structure = log.GetLog();

  auto block_list = log_structure[0];
  auto block = block_list.begin();

  // Check first entry
  {
    auto entry = block.operator*()->at(0);

    // Cast Deadline to millisecond
    ASSERT_EQ(entry.deadline / 1000000, 60);
    ASSERT_FALSE(entry.skip);
    ASSERT_EQ(entry.client_id, 2);
    ASSERT_EQ(entry.gsn, 0);
  }

  {
    auto entry = block.operator*()->at(1);

    // Cast Deadline to millisecond
    ASSERT_EQ(entry.deadline / 1000000, 60);
    ASSERT_FALSE(entry.skip);
    ASSERT_EQ(entry.client_id, 1);
    ASSERT_EQ(entry.gsn, 1);
  }
}

TEST_F(PessimisticSlotLogTest, TwoClientDifferentRegionsEpochTest) {
  PessimisticDMLog log("test", 1, milliseconds(100), configs_2reg_[0], region_latencies);

  // Create new Epoch
  auto epoch = new slog::internal::ZiplogRegionSlots();
  epoch->set_num_clients(2);
  epoch->set_gsn_base(0);

  AddClientSlots(epoch, 1, 2, 0);
  AddClientSlots(epoch, 2, 2, 1);

  log.AddEpoch(epoch, 0);

  // Check allocated slot order
  ASSERT_EQ(log.GetNumEntries(), 2);

  Blocks* log_structure = log.GetLog();

  auto block_list = log_structure[0];
  auto block = block_list.begin();

  // Check first entry
  {
    auto entry = block.operator*()->at(0);

    // Cast Deadline to millisecond
    ASSERT_EQ(entry.deadline / 1000000, 55);
    ASSERT_FALSE(entry.skip);
    ASSERT_EQ(entry.client_id, 1);
    ASSERT_EQ(entry.gsn, 0);
  }

  {
    auto entry = block.operator*()->at(1);

    // Cast Deadline to millisecond
    ASSERT_EQ(entry.deadline / 1000000, 60);
    ASSERT_FALSE(entry.skip);
    ASSERT_EQ(entry.client_id, 2);
    ASSERT_EQ(entry.gsn, 1);
  }
}

TEST_F(PessimisticSlotLogTest, TwoClientUnevenSlotsEpochTest) {
  PessimisticDMLog log("test", 1, milliseconds(100), configs_2reg_[0], region_latencies);

  // Create new Epoch
  auto epoch = new slog::internal::ZiplogRegionSlots();
  epoch->set_num_clients(2);
  epoch->set_gsn_base(0);

  AddClientSlots(epoch, 1, 3, 1);
  AddClientSlots(epoch, 2, 2, 1);

  log.AddEpoch(epoch, 0);

  // Check allocated slot order
  ASSERT_EQ(log.GetNumEntries(), 3);

  Blocks* log_structure = log.GetLog();

  auto block_list = log_structure[0];
  auto block = block_list.begin();

  // Check first entry
  {
    auto entry = block.operator*()->at(0);

    // Cast Deadline to millisecond
    ASSERT_EQ(entry.deadline / 1000000, 43);
    ASSERT_FALSE(entry.skip);
    ASSERT_EQ(entry.client_id, 1);
    ASSERT_EQ(entry.gsn, 0);
  }

  {
    auto entry = block.operator*()->at(1);

    // Cast Deadline to millisecond
    ASSERT_EQ(entry.deadline / 1000000, 60);
    ASSERT_FALSE(entry.skip);
    ASSERT_EQ(entry.client_id, 2);
    ASSERT_EQ(entry.gsn, 1);
  }

  {
    auto entry = block.operator*()->at(2);

    // Cast Deadline to millisecond
    ASSERT_EQ(entry.deadline / 1000000, 76);
    ASSERT_FALSE(entry.skip);
    ASSERT_EQ(entry.client_id, 1);
    ASSERT_EQ(entry.gsn, 2);
  }
}

TEST_F(PessimisticSlotLogTest, PendingSlotAllocation) {
  PessimisticDMLog log("test", 1, milliseconds(100), configs_2reg_[0], region_latencies);

  // Create new Epoch
  auto epoch = new slog::internal::ZiplogRegionSlots();
  epoch->set_num_clients(1);
  epoch->set_gsn_base(0);

  AddClientSlots(epoch, 1, 1, 1);

  log.AddEpoch(epoch, 0);

  // Client should not have any slots, as it will be allocated in the next epoch
  ASSERT_EQ(log.GetNumEntries(), 0);

  epoch->set_gsn_base(1);
  epoch->clear_slots();

  AddClientSlots(epoch, 1, 1, 1);

  log.AddEpoch(epoch, 100000000);
  ASSERT_EQ(log.GetNumEntries(), 1);

  Blocks* log_structure = log.GetLog();

  auto block_list = log_structure[0];
  auto block = block_list.begin();
  auto entry = block.operator*()->at(0);

  // Cast Deadline to millisecond
  ASSERT_EQ(entry.deadline / 1000000, 110);
  ASSERT_FALSE(entry.skip);
  ASSERT_EQ(entry.client_id, 1);
  ASSERT_EQ(entry.gsn, 0);
}


TEST_F(PessimisticSlotLogTest, ConsistentMultiSlotAllocation) {
  PessimisticDMLog log_r0("R0 ", 0, milliseconds(100), configs_3reg_[0], region_latencies);
  PessimisticDMLog log_r1("R1 ", 1, milliseconds(100), configs_3reg_[0], region_latencies);
  PessimisticDMLog log_r2("R2 ", 2, milliseconds(100), configs_3reg_[0], region_latencies);

  // Create new Epoch
  auto epoch = new slog::internal::ZiplogRegionSlots();
  epoch->set_num_clients(3);
  epoch->set_gsn_base(0);

  // Order (Client, Deadline, GSN):
  // C1 - 60 - 0
  // C0 - 65 - 1
  // C2 - 70 - 2
  AddClientSlots(epoch, 0, 2, 0);
  AddClientSlots(epoch, 1, 2, 1);
  AddClientSlots(epoch, 2, 2, 2);

  log_r0.AddEpoch(epoch, 0);
  log_r1.AddEpoch(epoch, 0);
  log_r2.AddEpoch(epoch, 0);

  auto x = {log_r0, log_r1, log_r2};

  for (auto log : x) {
    ASSERT_EQ(log.GetNumEntries(), 3);

    Blocks* log_structure = log.GetLog();

    auto block_list = log_structure[0];
    auto block = block_list.begin();

    std::vector<std::array<int, 3>> expected_result = {
        {1, 60, 0},
        {0, 65, 1},
        {2, 70, 2}
    };

    for (int result = 0; result < expected_result.size(); result++){
      auto entry = block.operator*()->at(result);

      auto [client_id, deadline, gsn] = expected_result[result];
      // Cast Deadline to millisecond
      ASSERT_EQ(entry.deadline / 1000000, deadline);
      ASSERT_FALSE(entry.skip);
      ASSERT_EQ(entry.client_id, client_id);
      ASSERT_EQ(entry.gsn, gsn);
    }

  }
}



TEST_F(PessimisticSlotLogTest, SimpleMultiSlotInsertion) {
  PessimisticDMLog log_r0("R0 ", 0, milliseconds(100), configs_2reg_[0], region_latencies);

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

TEST_F(PessimisticSlotLogTest, SimpleMultiSlotInsertionBatch) {
  PessimisticDMLog log_r0("R0 ", 0, milliseconds(100), configs_2reg_[0], region_latencies);

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

TEST_F(PessimisticSlotLogTest, SimpleMultiSlotInsertionPending) {
  PessimisticDMLog log_r0("R0 ", 0, milliseconds(100), configs_2reg_[0], region_latencies);

  {
    // Create new Epoch
    auto epoch = new slog::internal::ZiplogRegionSlots();
    epoch->set_num_clients(1);
    epoch->set_gsn_base(0);

    AddClientSlots(epoch, 1, 1, 0);

    log_r0.AddEpoch(epoch, 0);
  }


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

  ASSERT_FALSE(log_r0.HasNextBatch());
  {
    // Transaction must have been inserted in the first slot
    Blocks* log_structure = log_r0.GetLog();

    auto block_list = log_structure[0];
    auto block = block_list.begin();

    CheckEntry(block.operator*()->at(0), 0, 0, 0);
  }

  // Add next epoch to insert the txn
  {
    // Create new Epoch
    auto epoch = new slog::internal::ZiplogRegionSlots();
    epoch->set_num_clients(1);
    epoch->set_gsn_base(1);

    AddClientSlots(epoch, 1, 1, 0);

    log_r0.AddEpoch(epoch, 100000000);
  }

  {
    // Transaction must have been inserted in the first slot
    Blocks* log_structure = log_r0.GetLog();

    auto block_list = log_structure[0];
    auto block = block_list.begin();

    CheckEntry(block.operator*()->at(0), 105, 1, 0);
    auto result_batch = block.operator*()->at(0).batch;

    ASSERT_NE(result_batch, nullptr);

    ASSERT_EQ(1, result_batch->transactions_size());
    ASSERT_EQ(1000, result_batch->transactions(0).internal().id());
  }


}

// Client 0 touches on region 0 and 1
// Transaction should be inserted correctly in region 0 and 1, and skip on region 2
TEST_F(PessimisticSlotLogTest, MultiSlotInsertionMultiRegionSkip) {
  PessimisticDMLog log_r0("R0 ", 0, milliseconds(100), configs_3reg_[0], region_latencies);
  PessimisticDMLog log_r1("R1 ", 1, milliseconds(100), configs_3reg_[0], region_latencies);
  PessimisticDMLog log_r2("R2 ", 2, milliseconds(100), configs_3reg_[0], region_latencies);

  // Create new Epoch
  auto epoch = new slog::internal::ZiplogRegionSlots();
  epoch->set_num_clients(1);
  epoch->set_gsn_base(0);

  AddClientSlots(epoch, 1, 2, 0);

  log_r0.AddEpoch(epoch, 0);
  log_r1.AddEpoch(epoch, 0);
  log_r2.AddEpoch(epoch, 0);

  // Create new batch

  auto logs = {&log_r0, &log_r1, &log_r2};
  for (auto log : logs) {
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
    log->AddBatch(batch_info);
  }

  {
    // Transaction must have been inserted in the first slot
    Blocks* log_structure = log_r0.GetLog();

    auto block_list = log_structure[0];
    auto block = block_list.begin();

    CheckEntry(block.operator*()->at(0), 65, 1, 0);
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

    CheckEntry(block.operator*()->at(0), 65, 1, 0);
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

    CheckEntry(block.operator*()->at(0), 65, 1, 0, true);
    ASSERT_EQ(block.operator*()->at(0).batch, nullptr);
  }
}

// Client 0 touches on region 0 and 2
// Transaction should be inserted correctly in all regions
TEST_F(PessimisticSlotLogTest, MultiSlotInsertionInsertAll) {
  PessimisticDMLog log_r0("R0 ", 0, milliseconds(100), configs_3reg_[0], region_latencies);
  PessimisticDMLog log_r1("R1 ", 1, milliseconds(100), configs_3reg_[0], region_latencies);
  PessimisticDMLog log_r2("R2 ", 2, milliseconds(100), configs_3reg_[0], region_latencies);

  // Create new Epoch
  auto epoch = new slog::internal::ZiplogRegionSlots();
  epoch->set_num_clients(1);
  epoch->set_gsn_base(0);

  AddClientSlots(epoch, 1, 2, 0);

  log_r0.AddEpoch(epoch, 0);
  log_r1.AddEpoch(epoch, 0);
  log_r2.AddEpoch(epoch, 0);

  auto logs = {&log_r0, &log_r1, &log_r2};
  for (auto log : logs) {
    auto batch_info = new slog::internal::ForwardZiplogBatch();
    batch_info->set_generator(1);
    batch_info->set_slots(1);

    // Make Txn that touched only on the remote log
    auto expected_txn_1 = MakeTransaction({{"A"}, {"B", KeyType::WRITE}});
    expected_txn_1->mutable_internal()->clear_involved_regions();
    expected_txn_1->mutable_internal()->add_involved_regions(0);
    expected_txn_1->mutable_internal()->add_involved_regions(2);

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

    CheckEntry(block.operator*()->at(0), 65, 1, 0);
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

    CheckEntry(block.operator*()->at(0), 65, 1, 0, true);
    ASSERT_EQ(block.operator*()->at(0).batch, nullptr);
  }

  {
    // Transaction must have been inserted in the first slot
    Blocks* log_structure = log_r2.GetLog();

    auto block_list = log_structure[0];
    auto block = block_list.begin();

    CheckEntry(block.operator*()->at(0), 65, 1, 0);
    auto result_batch = block.operator*()->at(0).batch;

    ASSERT_NE(result_batch, nullptr);

    ASSERT_EQ(1, result_batch->transactions_size());
    ASSERT_EQ(1000, result_batch->transactions(0).internal().id());
  }
}

// Log Processing Tests



TEST_F(PessimisticSlotLogTest, OutOfOrderInsertion) {

  PessimisticDMLog log("test", 1, milliseconds(100), configs_3reg_[0], region_latencies);

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
    batch_info->set_generator(2);
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
    batch_info->set_generator(1);
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

TEST_F(PessimisticSlotLogTest, EmptyBatch) {
  PessimisticDMLog log_r0("R0 ", 0, milliseconds(100), configs_2reg_[0], region_latencies);

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

TEST_F(PessimisticSlotLogTest, SkipBatchWithThx) {

  PessimisticDMLog log_r0("R0 ", 0, milliseconds(100), configs_2reg_[0], region_latencies);

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


TEST_F(PessimisticSlotLogTest, SkipBatchWithNoop) {

  PessimisticDMLog log_r0("R0 ", 0, milliseconds(100), configs_2reg_[0], region_latencies);

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

  for (int i = 0; i < 2; i++){
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

  MultiSlotLog log("test", 1, milliseconds(100), configs_2reg_[0], region_latencies);

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
