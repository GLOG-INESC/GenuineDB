#include "execution/execution.h"

namespace slog {

MemOnlyStorageExecution::MemOnlyStorageExecution(const SharderPtr& sharder, const RegionId home, std::unique_ptr<Execution> && execution,
    const std::shared_ptr<Storage>& storage) : StorageExecution(sharder, home, std::move(execution)), storage_(storage) {

}

void MemOnlyStorageExecution::Execute(Transaction& txn) {

  if (execution_->Execute(txn)) {
    // Apply Writes
    for (const auto& kv : txn.keys()) {
      const auto& key = kv.key();
      const auto& value = kv.value_entry();
#ifdef PARTIAL_EXEC
      if (!(sharder_->is_local_key(key) && value.metadata().master() == home_) || value.type() == KeyType::READ){
        continue;
      }
#else
      if (!sharder->is_local_key(key) || value.type() == KeyType::READ) {
        continue;
      }
#endif

      Record new_record;
      new_record.SetMetadata(value.metadata());
      new_record.SetValue(value.new_value());
      storage_->Write(key, new_record);
    }
    for (const auto& key : txn.deleted_keys()) {
      storage_->Delete(key);
    }
  }
}

SpeculativeStorageExecution::SpeculativeStorageExecution(const SharderPtr& sharder, const RegionId home,
                                                         std::unique_ptr<Execution>&& execution,
                                                         const std::shared_ptr<SpeculativeMemOnlyStorage>& storage):
StorageExecution(sharder, home, std::move(execution)), storage_(storage) {

}

void SpeculativeStorageExecution::Execute(Transaction& txn, bool speculative) {

  if (execution_->Execute(txn)) {
    for (const auto& kv : txn.keys()) {
      const auto& key = kv.key();
      const auto& value = kv.value_entry();
#ifdef PARTIAL_EXEC
      if (!(sharder_->is_local_key(key) && value.metadata().master() == home_) || value.type() == KeyType::READ){
        continue;
      }
#else
      if (!sharder->is_local_key(key) || value.type() == KeyType::READ) {
        continue;
      }
#endif

      Record new_record;
      new_record.SetMetadata(value.metadata());
      new_record.SetValue(value.new_value());

      if (speculative) {
        storage_->SpeculativeWrite(key, new_record);
      } else {
        storage_->DefinitiveWrite(key, new_record);
      }
    }
    for (const auto& key : txn.deleted_keys()) {
      if (speculative) {
        storage_->SpeculativeDelete(key);
      } else {
        storage_->DefinitiveDelete(key);
      }
    }

  }

}

void SpeculativeStorageExecution::Rollback(Transaction& txn) {
    for (const auto& kv : txn.keys()) {
      const auto& key = kv.key();
      const auto& value = kv.value_entry();
#ifdef PARTIAL_EXEC
      if (!(sharder_->is_local_key(key) && value.metadata().master() == home_) || value.type() == KeyType::READ){
        continue;
      }
#else
      if (!sharder->is_local_key(key) || value.type() == KeyType::READ) {
        continue;
      }
#endif
      storage_->RollbackWrite(key);
    }

    for (const auto& key : txn.deleted_keys()) {
      storage_->RollbackDelete(key);
    }
}

}  // namespace slog