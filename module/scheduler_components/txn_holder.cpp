#include "module/scheduler_components/txn_holder.h"

namespace slog {

TxnHolder::TxnHolder(const ConfigurationPtr& config, Transaction* txn)
    : txn_id_(txn->internal().id()),
      main_txn_idx_(txn->internal().home()),
      lo_txns_(config->num_regions()),
      remaster_result_(std::nullopt),
      dispatchable_(true),
      aborting_(false),
      done_(false),
      num_lo_txns_(0),
#ifdef FULL_REP
      expected_num_lo_txns_(txn->internal().involved_regions_size()),
#else
      expected_num_lo_txns_(1),
#endif
      num_dispatches_(0),
      worker_(std::nullopt) {
  lo_txns_[main_txn_idx_].reset(txn);
  ++num_lo_txns_;
}

bool TxnHolder::AddLockOnlyTxn(Transaction* txn) {
  auto home = txn->internal().home();
  CHECK_LT(home, static_cast<int>(lo_txns_.size()));

  if (lo_txns_[home] != nullptr) {
    return false;
  }

  lo_txns_[home].reset(txn);

  ++num_lo_txns_;


  if (num_lo_txns_ == expected_num_lo_txns_){
    lo_txns_[main_txn_idx_]->mutable_internal()->clear_events();
    lo_txns_[main_txn_idx_]->mutable_internal()->mutable_events()->MergeFrom(lo_txns_[home]->internal().events());
  }


  return true;
}

Transaction* TxnHolder::FinalizeAndRelease() {
  auto& main_txn = lo_txns_[main_txn_idx_];
  auto main_internal = main_txn->mutable_internal();

  for (int i = 0; i < main_internal->events_size(); i++) {
    auto event = main_internal->mutable_events(i);
    event->set_home(main_internal->home());
  }

  for (auto& lo_txn : lo_txns_){
    if (lo_txn != nullptr && lo_txn != main_txn) {
      lo_txn.reset();
    }
  }
  // Do not use clear() here because lo_txns_ must never change in size
  return lo_txns_[main_txn_idx_].release();
}

}  // namespace slog