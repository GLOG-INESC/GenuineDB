#pragma once

#include <unordered_map>

#include "../storage/speculative_mem_only_storage.h"
#include "common/sharder.h"
#include "execution/execution.h"
#include "proto/transaction.pb.h"
#include "storage/storage.h"

namespace slog {

class Execution {
 public:
  virtual ~Execution() = default;
  // Returns whether the transaction was successfull or not
  virtual bool Execute(Transaction& txn) = 0;
};

class KeyValueExecution : public Execution {
 public:
  KeyValueExecution();
  bool Execute(Transaction& txn) final;
};

class NoopExecution : public Execution {
 public:
  bool Execute(Transaction& txn) final { txn.set_status(TransactionStatus::COMMITTED); return true; }
};

class TPCCExecution : public Execution {
 public:
  TPCCExecution();
  bool Execute(Transaction& txn) final;
};

class StorageExecution {
public:
  StorageExecution(const SharderPtr& sharder, const RegionId home, std::unique_ptr<Execution> && execution) :
    sharder_(sharder), home_(home), execution_(std::move(execution)){}
  virtual ~StorageExecution() = default;

protected:
  SharderPtr sharder_;
  RegionId home_;
  std::unique_ptr<Execution> execution_;

};

class MemOnlyStorageExecution : public StorageExecution {
public:
  MemOnlyStorageExecution(const SharderPtr& sharder, const RegionId home, std::unique_ptr<Execution> && execution,
    const std::shared_ptr<Storage>& storage);
  void Execute(Transaction& txn);
private:
  std::shared_ptr<Storage> storage_;

};

class SpeculativeStorageExecution : public StorageExecution {
public:
  SpeculativeStorageExecution(const SharderPtr& sharder, const RegionId home, std::unique_ptr<Execution> && execution,
    const std::shared_ptr<SpeculativeMemOnlyStorage>& storage);
  void Execute(Transaction& txn, bool speculative);
  void Rollback(Transaction& txn);

private:
  std::shared_ptr<SpeculativeMemOnlyStorage> storage_;
};

}  // namespace slog

