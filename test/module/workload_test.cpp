//
// Created by jrsoares on 23-01-2025.
//

#include <gtest/gtest.h>
#include "workload/skewed.h"
#include "test/test_utils.h"
#include "workload/basic.h"
#include "workload/unique.h"

using namespace std;
using namespace slog;

string WorkloadConfigGenerator(map<string, int> config){
  std::stringstream workload_config;
  auto x = 0;
  for (auto i : config){
    workload_config << i.first << "=" << i.second;

    if (x+1 != config.size()){
      workload_config << ",";
      x++;
    }
  }
  return workload_config.str();
}


TEST(YCSBTest, KeyTest){
  map<string, int> test_config = {{"hot", 10}, {"mp", 0}, {"mh", 0},{"records", 10},{"writes", 10},{"hot_records", 2},{"mh_homes", 2},{"mh_zipf", 1}};
  auto special_workload_configs = WorkloadConfigGenerator(test_config);

  auto custom_config = internal::Configuration();
  auto num_regs = 8;
  auto num_parts = 1;

  custom_config.mutable_simple_partitioning()->set_num_records(1000000*num_regs*num_parts);
  custom_config.mutable_simple_partitioning()->set_record_size_bytes(100);
  custom_config.set_glog_port(1);
  custom_config.set_order_port(2);
  auto config = MakeTestConfigurations("UNIQUE", num_regs /* num_regions */, 1 /* num_replicas */, num_parts /* num_partitions */,
                                       custom_config);


  std::mt19937 rg_(0);

  auto metadata_initializer = std::make_shared<SimpleMetadataInitializer>(num_regs, num_parts);


  for (int r = 0; r < num_regs; r++){
    for (int p = 0; p < num_parts; p++){
      auto sharder = Sharder::MakeSharder(config[p+r*num_parts]);

      auto region_key_list = RegionKeyList(config[p+r*num_parts], config[p+r*num_parts]->local_region(), config[p+r*num_parts]->local_partition(), 0.0, nullptr);
      for (int i = 0; i < 10000; i++){
        auto key = region_key_list.GetKey(rg_);
        ASSERT_EQ(metadata_initializer->Compute(key).master, config[p+r*num_parts]->local_region()) << "Key " << key << " Region " << r << " Partition " << p;
        ASSERT_TRUE(sharder->is_local_key(key));
      }
    }



  }

}




int main(int argc, char **argv){
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
/*
class WorkloadTest : public ::testing::Test {

};
 */