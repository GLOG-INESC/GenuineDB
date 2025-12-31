#pragma once

#include "common/concurrent_hash_map.h"
#include "glog/logging.h"
#include "storage/lookup_master_index.h"
#include "storage/storage.h"

namespace slog {
/*
 * Storage designed for Tiga Speculative Execution
 *
 * Tiga speculatively executes one write operation per key at a time;
 * This Storage keeps an extra backup_table to save the previous write result
 * in case a speculative transaction must be rolled back;
 */
class SpeculativeMemOnlyStorage : public Storage, public LookupMasterIndex {
 public:
  bool Read(const Key& key, Record& result) const final { return table_.Get(result, key); }

  // Both Write and Delete are mirrors for the definitive versions
  bool Write(const Key& key, const Record& record) final {
    return DefinitiveWrite(key, record);
  }

  bool Delete(const Key& key) final { return DefinitiveDelete(key); }

  bool SpeculativeWrite(const Key& key, const Record& record) {
    // Create backup and then update real table
    Record backup_record;
    // If the key existed in the table already, than move its value to the backup
    if (table_.Get(backup_record, key)) {
      backup_table_.InsertOrUpdate(key, backup_record);
    } else {
      // Special value to simbolize that upon rollback, it must be deleted
      backup_record.SetValue("delete");
      backup_table_.InsertOrUpdate(key, backup_record);
    }
    return table_.InsertOrUpdate(key, record);
  }

  // If the write is definitive, write immediately to both
  bool DefinitiveWrite(const Key& key, const Record& record) {
    auto insert_backup = backup_table_.InsertOrUpdate(key, record);
    auto insert_definitive = table_.InsertOrUpdate(key, record);

    return insert_backup && insert_definitive;
  }

  bool RollbackWrite(const Key& key) {
    Record backup_record;
    auto got_key = backup_table_.Get(backup_record, key);
    CHECK(got_key) << "Key does not exist in backup table during write rollback";
    if (backup_record.to_string() == "delete") {
      LOG(INFO) << "Rolling back first write";
      return table_.Erase(key);
    } else {
    return table_.InsertOrUpdate(key, backup_record);
    }
  }

  // No need to backup data since it is already done on Write
  bool SpeculativeDelete(const Key& key) { return table_.Erase(key); }

  bool DefinitiveDelete(const Key& key) {
    auto backup_erase = backup_table_.Erase(key);
    auto table_erase = table_.Erase(key);

    return backup_erase && table_erase;
  }

  bool RollbackDelete(const Key& key) {
    Record backup_record;
    auto got_key = backup_table_.Get(backup_record, key);
    CHECK(got_key) << "Key does not exist in backup table during delete rollback";

    return table_.InsertOrUpdate(key, backup_record);
  }

  bool GetMasterMetadata(const Key& key, Metadata& metadata) const final {
    Record rec;
    if (!table_.Get(rec, key)) {
      return false;
    }
    metadata = rec.metadata();
    return true;
  }

 private:
  ConcurrentHashMap<Key, Record> table_;
  ConcurrentHashMap<Key, Record> backup_table_;

};

}  // namespace slog