//
// Created by jrsoares on 19/09/25.
//

#include "ddr_lock_manager_common.h"

namespace slog {

std::optional<TxnId> LockQueueTail::AcquireReadLock(TxnId txn_id) {
  read_lock_requesters_.insert(txn_id);
  return write_lock_requester_;
}

std::vector<TxnId> LockQueueTail::AcquireWriteLock(TxnId txn_id) {
  std::vector<TxnId> deps;
  if (read_lock_requesters_.empty()) {
    if (write_lock_requester_.has_value()) {
      deps.push_back(write_lock_requester_.value());
    }
  } else {
    deps.insert(deps.end(), read_lock_requesters_.begin(), read_lock_requesters_.end());
    read_lock_requesters_.clear();
  }
  write_lock_requester_ = txn_id;
  return deps;
}

void LockQueueTail::UpdateDeadlockedLock(TxnId original_txn, TxnId dag_end) {

  // If the deadlocked txn is the current holder of the write lock, then update it
  if (write_lock_requester_.has_value() && write_lock_requester_.value() == original_txn) {
    write_lock_requester_ = dag_end;
  } else {
    // If instead the deadlocked txn has a read lock, also add dag_end to read locks

    if (!read_lock_requesters_.empty() && read_lock_requesters_.count(original_txn) == 1) {
      read_lock_requesters_.erase(original_txn);
      read_lock_requesters_.insert(dag_end);
    }
  }

}

}