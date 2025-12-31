#include "execution/tpcc/metadata_initializer.h"

#include <glog/logging.h>
#include "execution/tpcc/table.h"
namespace slog {
namespace tpcc {

TPCCMetadataInitializer::TPCCMetadataInitializer(uint32_t num_regions, uint32_t num_partitions, RegionId local_region)
    : num_regions_(num_regions), num_partitions_(num_partitions), local_region_(local_region) {}

Metadata TPCCMetadataInitializer::Compute(const Key& key) {
  CHECK_GE(key.size(), 4) << "Invalid key";
  uint32_t warehouse_id = *reinterpret_cast<const uint32_t*>(key.data());

  auto table_id = key[4];

  if (table_id == TableId::ITEM){
    // Item table is fully replicated, so always return local
    return Metadata(local_region_);
  }
  return Metadata(((warehouse_id - 1) / num_partitions_) % num_regions_);
}

}  // namespace tpcc
}  // namespace slog