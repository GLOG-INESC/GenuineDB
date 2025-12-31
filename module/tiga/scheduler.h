#pragma once

#include <glog/logging.h>

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/configuration.h"
#include "common/metrics.h"
#include "common/types.h"
#include "connection/broker.h"
#include "connection/sender.h"
#include "module/tiga/worker.h"
#include "storage/storage.h"
#include "module/tiga/TigaTxnHolder.h"

namespace tiga {

using slog::Broker;
using slog::EnvelopePtr;
using slog::MetricsRepositoryManagerPtr;
using slog::Storage;
using slog::Transaction;
using slog::internal::JanusDependency;
using slog::TxnId;


struct SeqEntry {
  uint64_t timestamp;
  TxnId txnId;
  slog::KeyType keyType;
};

struct SeqEntryCompare {
  bool operator()(const SeqEntry& a, const SeqEntry& b) const {
    if (a.timestamp != b.timestamp)
      return a.timestamp < b.timestamp;
    return a.txnId < b.txnId;
  }
};

using SequencerSet = std::set<SeqEntry, SeqEntryCompare>;

class Lock {
public:
  bool CanAcquireLock(const slog::KeyType & op_type) {
    if (op_type == slog::WRITE) {
      // Write lock can only succed if no transaction has the lock
      return lock_holders_.empty();
    } else {
      // If attempting a read lock, ensure the current holders are not a write lock
      return !is_write_;
    }
  }

  void AcquireLock(const TxnId & txn_id, const slog::KeyType & op_type) {
    CHECK(CanAcquireLock(op_type)) << "Txn " << txn_id << " cannot acquire lock";
    auto [lock_it, inserted] = lock_holders_.try_emplace(txn_id, 1);
    // A lock already existed for this txn
    // This can happen when a read-only txn is rolledback but because it is still at the top of the list,
    // It can re-acquire the lock again.
    // For this case, we associate a counter for the number of locks of each txn
    if (!inserted) {
      lock_it->second++;
    }
    is_write_ = op_type == slog::WRITE;
  }

  bool ReleaseLock(TxnId txn_id) {
    auto lock_it = lock_holders_.find(txn_id);
    CHECK(lock_it != lock_holders_.end()) << "Txn " << txn_id << " no longer has a lock";

    if (--lock_it->second == 0) {
      lock_holders_.erase(txn_id);
      if (lock_holders_.empty()) {
        is_write_ = false;
      }
    }

    return lock_holders_.empty();
  }
private:
  std::unordered_map<TxnId, int> lock_holders_;
  bool is_write_ = false;
};

class Scheduler : public slog::NetworkedModule {
 public:
  Scheduler(const std::shared_ptr<Broker>& broker, const std::shared_ptr<SpeculativeMemOnlyStorage>& storage,
            const MetricsRepositoryManagerPtr& metrics_manager,
            std::chrono::milliseconds poll_timeout = slog::kModuleTimeout, bool debug_mode = false);

  std::string name() const override { return "Scheduler"; }

 protected:
  void Initialize() final;

  void OnInternalRequestReceived(EnvelopePtr&& env) final;
  void OnInternalResponseReceived(EnvelopePtr&& env) final;

  // Handle responses from the workers
  bool OnCustomSocket() final;

 private:
  void ProcessTransaction(EnvelopePtr&& env);
  void ProcessAgreementConfirmation(EnvelopePtr&& env);
  void ProcessExecutionConfirmation(TxnId txn_id, bool is_rollback);
  void ReleaseLocks(TigaTxnHolder* txn);
  void CheckTxnForExecution(TigaTxnHolder* txn);
  void PrintStats(EnvelopePtr&& env);
  void HandleWrongSpeculation(TigaTxnHolder* txn_holder, std::vector<TxnId>& candidate_txns, uint64_t definitive_ts);


  // This must be defined at the end so that the workers exit before any resources
  // in the scheduler is destroyed
  std::vector<std::unique_ptr<slog::ModuleRunner>> workers_;
  int current_worker_;

  string scheduler_id_debug_;

  std::unordered_map<slog::Key, uint64_t> latest_writing_txns_;
  std::unordered_map<slog::Key, uint64_t> latest_reading_txns_;

  std::unordered_map<TxnId, TigaTxnHolder*> txns_;



  std::unordered_map<slog::Key, SequencerSet> execSequencer_;
  std::unordered_map<slog::Key, Lock> entriesInSpec_;

  bool debug_mode_ = false;
};

}  // namespace janus