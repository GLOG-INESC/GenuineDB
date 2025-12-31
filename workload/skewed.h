#pragma once

#include <vector>

#include "common/configuration.h"
#include "common/string_utils.h"
#include "common/types.h"
#include "proto/transaction.pb.h"
#include "workload/workload.h"

using std::vector;

namespace slog {

/**
 * SkewedWorkload is a debugging workload to test the system in the most skewed environment possible.
 * Transactions will always choose the same set of keys for each partition and home
 * Hot/Cold values are not considered for this workload
 * */
class SkewedWorkload : public Workload {
 public:
  SkewedWorkload(const ConfigurationPtr& config, RegionId region, ReplicaId replica, const std::string& data_dir,
                const std::string& params_str, const uint32_t seed = std::random_device()(),
                const RawParamMap& extra_default_params = {});

  std::pair<Transaction*, TransactionProfile> NextTransaction();

 protected:
  int local_region() { return config_->num_regions() == 1 ? local_replica_ : local_region_; }

  ConfigurationPtr config_;
  RegionId local_region_;
  ReplicaId local_replica_;
  std::vector<int> distance_ranking_;
  int zipf_coef_;

  // This is an index of keys by their partition and home.
  // Each partition holds a vector of homes, each of which
  // is a list of keys.
  vector<vector<KeyList>> partition_to_key_lists_;

  std::mt19937 rg_;
  RandomStringGenerator rnd_str_;

  TxnId client_txn_id_counter_;

  bool is_one_shot_workload_;
};

}  // namespace slog