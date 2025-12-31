//
// Created by jrsoares on 20/10/25.
//
#include <gtest/gtest.h>
#include "storage/mem_only_storage.h"
#include "execution/tpcc/metadata_initializer.h"
#include "execution/tpcc/load_tables.h"
#include "execution/tpcc/transaction.h"
#include "common/configuration.h"
#include "common/sharder.h"
#include "common/proto_utils.h"
#include "test/test_utils.h"

using namespace std;
using namespace slog;
using namespace slog::tpcc;

class TPCCPartialRepTest : public ::testing::Test {
 protected:
  void SetUp() {
    storage = std::make_shared<MemOnlyStorage>();
    // Load with two regions, thus 2 warehouses
    metadata_initializer = std::make_shared<TPCCMetadataInitializer>(4, 1);
    //auto kv_storage_adapter = std::make_shared<KVStorageAdapter>(storage, metadata_initializer);
    //LoadTables(kv_storage_adapter, 2, 2, 1, 0);
    auto asd = internal::Configuration();
    asd.mutable_tpcc_partitioning()->set_warehouses(4);
    asd.set_execution_type(slog::internal::TPC_C);
    asd.set_glog_port(7);
    asd.set_order_port(7);

    config = MakeTestConfigurations("asd", 2, 1, 1, asd)[0];

    sharder = Sharder::MakeSharder(config);
  }

  void PopulateRegionMetadata(){
    for (auto& kv : *txn.mutable_keys()) {
      auto key = kv.key();
      auto value = kv.mutable_value_entry();

      auto new_metadata = metadata_initializer->Compute(key);
      value->mutable_metadata()->set_master(new_metadata.master);
      value->mutable_metadata()->set_counter(new_metadata.counter);
    }
  }

  std::shared_ptr<Storage> storage;
  Transaction txn;
  std::shared_ptr<MetadataInitializer> metadata_initializer;
  ConfigurationPtr config;
  std::shared_ptr<Sharder> sharder;

};

TEST_F(TPCCPartialRepTest, NewOrderSingleRegion) {


  auto key_gen_adapter = std::make_shared<TxnKeyGenStorageAdapter>(txn);

  int w_id = 1;
  int d_id = 2;
  int c_id = 5;
  int o_id = 5000;
  int64_t datetime = 1234567890;
  int w_i_id = 1;
  std::array<NewOrderTxn::OrderLine, kLinePerOrder> ol;
  for (int i = 0; i < static_cast<int>(ol.size()); i++) {
    ol[i] = NewOrderTxn::OrderLine{.id = i + 1, .supply_w_id = w_id, .item_id = (i + 1) * 10, .quantity = 4};
  }
  {
    NewOrderTxn new_order_txn(key_gen_adapter, w_id, d_id, c_id, o_id, datetime, w_i_id, ol);
    new_order_txn.Read();
    new_order_txn.Write();
    key_gen_adapter->Finialize();
  }

  // Populate Involved Regions and Partitions
  PopulateInvolvedMetadata(metadata_initializer, txn);
  PopulateInvolvedPartitions(sharder, txn);


  ASSERT_EQ(txn.internal().involved_partitions_size(), 1);

  RegionId local_region = -1;

  for (auto key : txn.keys()){
    if (local_region == (RegionId)-1){
      local_region = metadata_initializer->Compute(key.key()).master;
    } else {
      ASSERT_EQ(metadata_initializer->Compute(key.key()).master, local_region);
    }
  }

  // Create a NewOrder txn
  // Check that Other keys correspond to the same shard/region than the warehouse


}

TEST_F(TPCCPartialRepTest, NewOrderMultiRegion) {
  auto key_gen_adapter = std::make_shared<TxnKeyGenStorageAdapter>(txn);

  int w_id = 1;
  int d_id = 2;
  int c_id = 5;
  int o_id = 5000;
  int64_t datetime = 1234567890;
  int w_i_id = 1;
  std::array<NewOrderTxn::OrderLine, kLinePerOrder> ol;
  for (int i = 0; i < static_cast<int>(ol.size()); i++) {
    ol[i] = NewOrderTxn::OrderLine{.id = i + 1, .supply_w_id = 2, .item_id = (i + 1) * 10, .quantity = 4};
  }
  {
    NewOrderTxn new_order_txn(key_gen_adapter, w_id, d_id, c_id, o_id, datetime, w_i_id, ol);
    new_order_txn.Read();
    new_order_txn.Write();
    key_gen_adapter->Finialize();
  }

  // Populate Involved Regions and Partitions
  PopulateInvolvedMetadata(metadata_initializer, txn);
  PopulateInvolvedPartitions(sharder, txn);

  ASSERT_EQ(txn.internal().involved_partitions_size(), 2);

  std::set<RegionId> regions;
  std::map<RegionId, std::vector<Key>> partitioned_keys;
  for (auto key : txn.keys()){
    auto master = metadata_initializer->Compute(key.key()).master;
    regions.insert(master);
    partitioned_keys[master].push_back(key.key());
  }

  ASSERT_EQ(regions.size(), 2);

}

TEST_F(TPCCPartialRepTest, Payment) {
  auto key_gen_adapter = std::make_shared<TxnKeyGenStorageAdapter>(txn);


  int w_id = 1;
  int d_id = 2;
  int c_w_id = 1;
  int c_d_id = 3;
  int c_id = 4;
  int64_t amount = 10000;
  int64_t datetime = 1234567890;
  int h_id = 33333;

  PaymentTxn payment_txn(key_gen_adapter, w_id, d_id, c_w_id, c_d_id, c_id, amount, datetime, h_id);
  payment_txn.Read();
  payment_txn.Write();
  key_gen_adapter->Finialize();

  // Populate Involved Regions and Partitions
  PopulateInvolvedMetadata(metadata_initializer, txn);
  PopulateInvolvedPartitions(sharder, txn);


  ASSERT_EQ(txn.internal().involved_partitions_size(), 1);

  RegionId local_region = -1;

  for (auto key : txn.keys()){
    if (local_region == (RegionId)-1){
      local_region = metadata_initializer->Compute(key.key()).master;
    } else {
      ASSERT_EQ(metadata_initializer->Compute(key.key()).master, local_region);
    }
  }

}

TEST_F(TPCCPartialRepTest, RemotePayment) {
  auto key_gen_adapter = std::make_shared<TxnKeyGenStorageAdapter>(txn);


  int w_id = 1;
  int d_id = 2;
  int c_w_id = 2;
  int c_d_id = 3;
  int c_id = 4;
  int64_t amount = 10000;
  int64_t datetime = 1234567890;
  int h_id = 33333;

  PaymentTxn payment_txn(key_gen_adapter, w_id, d_id, c_w_id, c_d_id, c_id, amount, datetime, h_id);
  payment_txn.Read();
  payment_txn.Write();
  key_gen_adapter->Finialize();

  // Populate Involved Regions and Partitions
  PopulateInvolvedMetadata(metadata_initializer, txn);
  PopulateInvolvedPartitions(sharder, txn);


  ASSERT_EQ(txn.internal().involved_partitions_size(), 2);

  std::set<RegionId> regions;
  std::map<RegionId, std::vector<Key>> partitioned_keys;
  for (auto key : txn.keys()){
    auto master = metadata_initializer->Compute(key.key()).master;
    regions.insert(master);
    partitioned_keys[master].push_back(key.key());
  }

  ASSERT_EQ(regions.size(), 2);

}

TEST_F(TPCCPartialRepTest, OrderStatus) {
  auto key_gen_adapter = std::make_shared<TxnKeyGenStorageAdapter>(txn);


  int w_id = 1;
  int d_id = 2;
  int c_id = 2;
  int o_id = 3;
  OrderStatusTxn order_status_txn(key_gen_adapter, w_id, d_id, c_id, o_id);
  order_status_txn.Read();
  order_status_txn.Write();
  key_gen_adapter->Finialize();

  // Populate Involved Regions and Partitions
  PopulateInvolvedMetadata(metadata_initializer, txn);
  PopulateInvolvedPartitions(sharder, txn);

  ASSERT_EQ(txn.internal().involved_partitions_size(), 1);
  RegionId local_region = -1;

  for (auto key : txn.keys()){
    if (local_region == (RegionId)-1){
      local_region = metadata_initializer->Compute(key.key()).master;
    } else {
      ASSERT_EQ(metadata_initializer->Compute(key.key()).master, local_region);
    }
  }
}

TEST_F(TPCCPartialRepTest, Deliver) {
  auto key_gen_adapter = std::make_shared<TxnKeyGenStorageAdapter>(txn);

  int w_id = 1;
  int d_id = 2;
  int no_o_id = 2;
  int c_id = 3;
  int o_carrier = 123;
  int64_t datetime = 1234567890;
  DeliverTxn deliver(key_gen_adapter, w_id, d_id, no_o_id, c_id, o_carrier, datetime);
  deliver.Read();
  deliver.Write();
  key_gen_adapter->Finialize();

  // Populate Involved Regions and Partitions
  PopulateInvolvedMetadata(metadata_initializer, txn);
  PopulateInvolvedPartitions(sharder, txn);

  ASSERT_EQ(txn.internal().involved_partitions_size(), 1);

  RegionId local_region = -1;

  for (auto key : txn.keys()){
    if (local_region == (RegionId)-1){
      local_region = metadata_initializer->Compute(key.key()).master;
    } else {
      ASSERT_EQ(metadata_initializer->Compute(key.key()).master, local_region);
    }
  }
}

TEST_F(TPCCPartialRepTest, StockLevel) {
  auto key_gen_adapter = std::make_shared<TxnKeyGenStorageAdapter>(txn);

  int w_id = 1;
  int d_id = 2;
  int o_id = 2;
  std::array<int, StockLevelTxn::kTotalItems> i_ids;
  for (int i = 0; i < StockLevelTxn::kTotalItems; i++) {
    i_ids[i] = i;
  }
  StockLevelTxn stock_level(key_gen_adapter, w_id, d_id, o_id, i_ids);
  stock_level.Read();
  stock_level.Write();
  key_gen_adapter->Finialize();

  // Populate Involved Regions and Partitions
  PopulateInvolvedMetadata(metadata_initializer, txn);
  PopulateInvolvedPartitions(sharder, txn);


  ASSERT_EQ(txn.internal().involved_partitions_size(), 1);
}