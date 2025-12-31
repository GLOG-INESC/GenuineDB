#pragma once

#include <vector>

#include "common/configuration.h"
#include "common/string_utils.h"
#include "common/types.h"
#include "proto/transaction.pb.h"
#include "workload/workload.h"

using std::vector;

namespace slog {

class YCSBWorkload : public Workload {
 public:
  YCSBWorkload(const ConfigurationPtr& config, RegionId region, ReplicaId replica, const std::string& data_dir,
                const std::string& params_str, const std::shared_ptr<ZipfDistribution>& zipf_dist, const uint32_t seed = std::random_device()(),
                const RawParamMap& extra_default_params = {});

  virtual std::pair<Transaction*, TransactionProfile> NextTransaction();

 protected:
  int local_region() { return config_->num_regions() == 1 ? local_replica_ : local_region_; }

  ConfigurationPtr config_;
  RegionId local_region_;
  ReplicaId local_replica_;
  std::vector<int> distance_ranking_;
  double zipf_access_;
  int zipf_coef_;

  // This is an index of keys by their home.
  // Each home holds a vector of keys, which can be drawn by a given distribution
  vector<vector<RegionKeyList>> region_to_key_lists_;

  std::mt19937 rg_;
  RandomStringGenerator rnd_str_;

  TxnId client_txn_id_counter_;

  bool is_one_shot_workload_ = false;
  std::vector<int> txn_mix_;

 private:
  Transaction* ReadTransaction(TransactionProfile& pro);
  Transaction* WriteTransaction(TransactionProfile& pro);
  Transaction* ReadModifyWriteTransaction(TransactionProfile& pro);

  vector<uint32_t> SelectHomes(bool is_mh);
  vector<uint32_t> SelectPartitions(bool is_mp);

};

}  // namespace slog