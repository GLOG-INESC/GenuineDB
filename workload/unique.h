#pragma once

#include <vector>
#include <list>
#include "common/configuration.h"
#include "common/string_utils.h"
#include "common/types.h"
#include "proto/transaction.pb.h"
#include "workload/basic.h"

using std::vector;
using std::list;

namespace slog {

class UniqueWorkload : public BasicWorkload {
 public:
  UniqueWorkload(const ConfigurationPtr& config, RegionId region, ReplicaId replica, const std::string& data_dir,
                const std::string& params_str, const uint64_t num_generators, const uint32_t workload_id, const uint64_t num_clients,
                const RawParamMap& extra_default_params = {});

  std::pair<Transaction*, TransactionProfile> NextTransaction() override;

  // Debugging Purposes
  std::pair<Transaction*, TransactionProfile> SpecificTransaction(std::vector<RegionId> regions, std::vector<PartitionId> partitions);


 private:
  uint64_t workload_id_;
  uint64_t counter_ = 0;
  uint64_t num_generators_;
  vector<vector<uint64_t>> counters_;
};

}  // namespace slog