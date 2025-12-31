//
// Created by jrsoares on 19/09/25.
//
#pragma once

#include "common/types.h"
namespace slog {
/**
 * An object of this class represents the tail of the lock queue.
 * We don't update this structure when a transaction releases its
 * locks. Therefore, this structure might contain released transactions
 * so we need to verify any result returned from it.
 */
class LockQueueTail {
 public:
  std::optional<TxnId> AcquireReadLock(TxnId txn_id);
  std::vector<TxnId> AcquireWriteLock(TxnId txn_id);

  // If a transaction in a deadlock held a
  void UpdateDeadlockedLock(TxnId original_txn, TxnId dag_end);

  /* For debugging */
  std::optional<TxnId> write_lock_requester() const { return write_lock_requester_; }

  /* For debugging */
  std::unordered_set<TxnId> read_lock_requesters() const { return read_lock_requesters_; }

 private:
  std::optional<TxnId> write_lock_requester_;
  std::unordered_set<TxnId> read_lock_requesters_;
};
}

