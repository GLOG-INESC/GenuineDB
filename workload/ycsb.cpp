#include "workload/ycsb.h"

#include <fcntl.h>
#include <glog/logging.h>

#include <algorithm>
#include <iomanip>
#include <random>
#include <sstream>
#include <unordered_set>

#include "common/offline_data_reader.h"
#include "common/proto_utils.h"
#include "proto/offline_data.pb.h"

using std::bernoulli_distribution;
using std::iota;
using std::sample;
using std::unordered_set;

namespace slog {
namespace {

// Percentage of multi-home transactions
constexpr char MH_PCT[] = "mh";
// Max number of regions selected as homes in a multi-home transaction
constexpr char MH_HOMES[] = "mh_homes";
// Zipf coefficient for selecting regions to access in a txn. Must be non-negative.
// The lower this is, the more uniform the regions are selected
constexpr char MH_ZIPF[] = "mh_zipf";
// Percentage of multi-partition transactions

// Percentage of multi-partition transactions
constexpr char MP_PCT[] = "mp";
// Max number of partitions selected as parts of a multi-partition transaction
constexpr char MP_PARTS[] = "mp_parts";

// Contention level of accesses according to a zipfian distribution.
// A zipfian of 0 produces an uniform distribution
constexpr char ZIPF[] = "zipf";
// Number of records in a transaction
constexpr char RECORDS[] = "records";
// Size of a written value in bytes
constexpr char VALUE_SIZE[] = "value_size";
// If set to 1, a SH txn will always be sent to the nearest
// region, a MH txn will always have a part that touches the nearest region
constexpr char NEAREST[] = "nearest";
// Partition that is used in a single-partition transaction.
// Use a negative number to select a random partition for
// each transaction
constexpr char SP_PARTITION[] = "sp_partition";
// Home that is used in a single-home transaction.
// The NEAREST parameter is ignored if this is positive
constexpr char SH_HOME[] = "sh_home";

// Transaction mix to choose which YCSB version we wish to emulate.
// Ration is measured as "Write% : Read% : ReadModifyWrite%"
// The sum of all ratios should be equal to 1
// Default is YCSB-F (100% read-modify-write)
constexpr char TXN_MIX[] = "mix";

// Percentage of read-modify-write transactions that are 2FI (i.e. they have read dependencies)
constexpr char FI_PCT[] = "fi";


const RawParamMap DEFAULT_PARAMS = {{MH_PCT, "0"},   {MH_HOMES, "2"},    {MH_ZIPF, "0"},  {ZIPF, "0.0"},          {RECORDS, "10"},{MP_PCT, "0"},
                                    {MP_PARTS, "2"},
                                    {VALUE_SIZE, "100"}, {NEAREST, "1"},  {SP_PARTITION, "-1"},
                                    {SH_HOME, "-1"}, {TXN_MIX, "0_0_100"}, {FI_PCT, "0"}};

// For the Calvin experiment, there is a single region, so replace the regions by the replicas so that
// we generate the same workload as other experiments
int GetNumRegions(const ConfigurationPtr& config) {
  return config->num_regions() == 1 ? config->num_replicas(config->local_region()) : config->num_regions();
}

}  // namespace

YCSBWorkload::YCSBWorkload(const ConfigurationPtr& config, RegionId region, ReplicaId replica, const string& data_dir,
                             const string& params_str, const std::shared_ptr<ZipfDistribution>& zipf_dist, const uint32_t seed,  const RawParamMap& extra_default_params
                           )
    : Workload(MergeParams(extra_default_params, DEFAULT_PARAMS), params_str),
      config_(config),
      local_region_(region),
      local_replica_(replica),
      distance_ranking_(config->distance_ranking_from(region)),
      zipf_access_(params_.GetDouble(ZIPF)),
      zipf_coef_(params_.GetInt32(MH_ZIPF)),
      rg_(seed),
      rnd_str_(seed),
      client_txn_id_counter_(0) {
  name_ = "ycsb";
  auto num_regions = GetNumRegions(config);

  const auto& proto_config = config->proto_config();
  region_to_key_lists_.resize(num_regions);
  for (int reg = 0; reg < num_regions; reg++ ) {
    for (int p = 0; p < config->num_partitions(); p++){

      switch (proto_config.partitioning_case()) {
        case internal::Configuration::kSimplePartitioning: {
          region_to_key_lists_[reg].emplace_back(config, reg, p, zipf_access_, zipf_dist);
          break;
        }
        default:
          LOG(FATAL) << "Invalid partioning mode: "
                     << CASE_NAME(proto_config.partitioning_case(), internal::Configuration);
      }
    }

  }

  LOG(INFO) << "REGIONS COMPLETE";


  if (distance_ranking_.empty()) {
    for (size_t i = 0; i < static_cast<size_t>(num_regions); i++) {
      if (i != static_cast<size_t>(local_region())) {
        distance_ranking_.push_back(i);
      }
    }
    if (zipf_coef_ > 0) {
      LOG(WARNING) << "Distance ranking is not provided. MH_ZIPF is reset to 0.";
      zipf_coef_ = 0;
    }
  } else if (config_->num_regions() == 1) {
    // This case is for the Calvin experiment where there is only a single region.
    // The num_regions variable is equal to num_replicas at this point
    CHECK_EQ(distance_ranking_.size(), num_regions * (num_regions - 1));
    size_t from = local_region() * (num_regions - 1);
    std::copy_n(distance_ranking_.begin() + from, num_regions, distance_ranking_.begin());
    distance_ranking_.resize(num_regions - 1);
  }

  CHECK_EQ(distance_ranking_.size(), num_regions - 1) << "Distance ranking size must match the number of regions";

  if (!params_.GetInt32(NEAREST)) {
    distance_ranking_.insert(distance_ranking_.begin(), local_region());
  }

  auto txn_mix_str = Split(params_.GetString(TXN_MIX), "_");
  CHECK_EQ(txn_mix_str.size(), 3) << "There must be exactly 3 values for txn mix";
  for (const auto& t : txn_mix_str) {
    txn_mix_.push_back(std::stoi(t));
  }

  auto total_percentage = 0;

  for (auto ratio : txn_mix_){
    total_percentage += ratio;
  }

  CHECK_EQ(total_percentage, 100) << "All ratios must sum to 100";

  if (proto_config.partitioning_case() == internal::Configuration::kHashPartitioning) {
    LOG(FATAL) << "Invalid partioning mode: "
               << CASE_NAME(proto_config.partitioning_case(), internal::Configuration);
  }
}

std::pair<Transaction*, TransactionProfile> YCSBWorkload::NextTransaction() {
  TransactionProfile pro;

  pro.client_txn_id = client_txn_id_counter_;

  Transaction* txn;
  std::discrete_distribution<> select_ycsb_txn(txn_mix_.begin(), txn_mix_.end());
  switch (select_ycsb_txn(rg_)) {
    case 0:
      txn = ReadTransaction(pro);
      break;
    case 1:
      txn = WriteTransaction(pro);
      break;
    case 2:
      txn = ReadModifyWriteTransaction(pro);
      break;
    default:
      LOG(FATAL) << "Invalid txn choice";
  }


  return {txn, pro};
}

vector<uint32_t> YCSBWorkload::SelectHomes(bool is_mh){
  // Decide if this is a multi-home txn or not
  auto num_regions = GetNumRegions(config_);

  // Select a number of homes to choose from for each record
  vector<uint32_t> selected_homes;
  if (is_mh) {
    CHECK_GE(num_regions, 2) << "There must be at least 2 regions for MH txns";
    auto max_num_homes = std::min(params_.GetInt32(MH_HOMES), num_regions);

    CHECK_GE(max_num_homes, 2) << "At least 2 regions must be selected for MH txns";
    auto num_homes = std::uniform_int_distribution{2, max_num_homes}(rg_);
    selected_homes.reserve(num_homes);

    if (params_.GetInt32(NEAREST)) {
      selected_homes.push_back(local_region());
      num_homes--;
    }
    auto sampled_homes = zipf_sample(rg_, zipf_coef_, distance_ranking_, num_homes);
    selected_homes.insert(selected_homes.end(), sampled_homes.begin(), sampled_homes.end());
  } else {
    if (params_.GetInt32(SH_HOME) >= 0) {
      selected_homes.push_back(params_.GetInt32(SH_HOME));
    } else {
      if (params_.GetInt32(NEAREST)) {
        selected_homes.push_back(local_region());
      } else {
        std::uniform_int_distribution<uint32_t> dis(0, num_regions - 1);
        selected_homes.push_back(dis(rg_));
      }
    }
  }

  return selected_homes;
}

vector<uint32_t> YCSBWorkload::SelectPartitions(bool is_mp){

  auto num_partitions = config_->num_partitions();

  // Select a number of partitions to choose from for each record
  vector<uint32_t> selected_partitions;
  if (is_mp) {
    CHECK_GE(num_partitions, 2) << "There must be at least 2 partitions for MP txns";

    // Generate a permuation of 0..num_partitions-1
    selected_partitions.resize(num_partitions);
    iota(selected_partitions.begin(), selected_partitions.end(), 0);
    shuffle(selected_partitions.begin(), selected_partitions.end(), rg_);

    // Compute number of needed partitions
    auto max_num_partitions = std::min(num_partitions, params_.GetInt32(MP_PARTS));
    CHECK_GE(max_num_partitions, 2) << "At least 2 partitions must be selected for MP txns";

    // Select the first max_num_partitions
    std::uniform_int_distribution parts(2, max_num_partitions);
    selected_partitions.resize(parts(rg_));
  } else {
    auto sp_partition = params_.GetInt32(SP_PARTITION);
    if (sp_partition < 0) {
      // Pick a random partition if no specific partition is given
      std::uniform_int_distribution<uint32_t> dis(0, num_partitions - 1);
      selected_partitions.push_back(dis(rg_));
    } else {
      // Use the given partition
      CHECK_LT(static_cast<uint32_t>(sp_partition), num_partitions)
          << "Selected single-partition partition does not exist";
      selected_partitions.push_back(sp_partition);
    }
  }

  return selected_partitions;
}

Transaction* YCSBWorkload::ReadTransaction(slog::TransactionProfile& pro) {
  // Decide if this is a multi-home txn or not
  auto multi_home_pct = params_.GetDouble(MH_PCT);
  bernoulli_distribution is_mh(multi_home_pct / 100);
  pro.is_multi_home = is_mh(rg_);

  auto multi_part_pct = params_.GetDouble(MP_PCT);
  bernoulli_distribution is_mp(multi_part_pct / 100);
  pro.is_multi_partition = is_mp(rg_);

  pro.is_rw = false;
  pro.is_2fi = false;

  // Select a number of homes to choose from for each record
  vector<uint32_t> selected_homes = SelectHomes(pro.is_multi_home);
  vector<uint32_t> selected_partitions = SelectPartitions(pro.is_multi_partition);

  vector<KeyMetadata> keys;
  vector<vector<string>> code;
  auto records = params_.GetUInt32(RECORDS);
  for (size_t i = 0; i < records; i++) {

    auto partition = selected_partitions[i % selected_partitions.size()];

    // Evenly divide the records to the selected homes
    auto home = selected_homes[i / ((records + 1) / selected_homes.size())];
    for (;;) {
      Key key = region_to_key_lists_[home][partition].GetKey(rg_);

      auto ins = pro.records.try_emplace(key, TransactionProfile::Record());
      if (ins.second) {
        auto& record = ins.first->second;

        code.push_back({"GET", key});
        keys.emplace_back(key, KeyType::READ);
        record.is_write = false;
        record.home = home;
        break;
      }
    }
  }

  auto txn = MakeTransaction(keys, code);
  txn->mutable_internal()->set_id(client_txn_id_counter_);
  txn->mutable_internal()->set_is_one_shot_txn(true);
  client_txn_id_counter_++;

  return txn;
}

Transaction* YCSBWorkload::WriteTransaction(slog::TransactionProfile& pro) {
  // Decide if this is a multi-home txn or not
  auto multi_home_pct = params_.GetDouble(MH_PCT);
  bernoulli_distribution is_mh(multi_home_pct / 100);
  pro.is_multi_home = is_mh(rg_);

  auto multi_part_pct = params_.GetDouble(MP_PCT);
  bernoulli_distribution is_mp(multi_part_pct / 100);
  pro.is_multi_partition = is_mp(rg_);

  // TODO - make this clarification better
  pro.is_rw = true;
  pro.is_2fi = false;

  // Select a number of homes to choose from for each record
  vector<uint32_t> selected_homes = SelectHomes(pro.is_multi_home);
  vector<uint32_t> selected_partitions = SelectPartitions(pro.is_multi_partition);

  vector<KeyMetadata> keys;
  vector<vector<string>> code;
  auto records = params_.GetUInt32(RECORDS);
  auto value_size = params_.GetUInt32(VALUE_SIZE);
  for (size_t i = 0; i < records; i++) {
    // Evenly divide the records to the selected homes
    auto partition = selected_partitions[i % selected_partitions.size()];

    auto home = selected_homes[i / ((records + 1) / selected_homes.size())];

    for (;;) {
      Key key = region_to_key_lists_[home][partition].GetKey(rg_);

      auto ins = pro.records.try_emplace(key, TransactionProfile::Record());
      if (ins.second) {
        auto& record = ins.first->second;

        code.push_back({"SET", key, rnd_str_(value_size)});
        keys.emplace_back(key, KeyType::WRITE);
        record.is_write = true;
        record.home = home;
        break;
      }
    }
  }

  auto txn = MakeTransaction(keys, code);
  txn->mutable_internal()->set_id(client_txn_id_counter_);
  txn->mutable_internal()->set_is_one_shot_txn(true);
  client_txn_id_counter_++;

  return txn;

}

// Write transactions imply reading the value, however they do not require exchanging the read sets
// This difference is done via the "is_one_shot_txn" flag
Transaction* YCSBWorkload::ReadModifyWriteTransaction(slog::TransactionProfile& pro) {
  Transaction* txn = WriteTransaction(pro);

  // Decide if this is a multi-home txn or not
  auto fi_pct = params_.GetDouble(FI_PCT);
  bernoulli_distribution is_fi(fi_pct / 100);
  pro.is_2fi = is_fi(rg_);

  txn->mutable_internal()->set_is_one_shot_txn(!pro.is_2fi);

  return txn;
}
}  // namespace slog