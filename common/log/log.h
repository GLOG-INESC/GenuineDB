//
// Created by jrsoares on 14-08-2024.
//

#pragma once

#include <memory>
#include <string>

#include "common/async_log.h"
#include "common/types.h"
#include "proto/internal.pb.h"

namespace slog {

using namespace std;
using BatchPtr = std::unique_ptr<Batch>;

struct ClientInfo {
  ClientId id;
  TransactionType client_type;
  uint32_t region;
  uint64_t interval_between_slots;

  // Overloading the < operator for comparisons
  bool operator<(const ClientInfo& other) const {
    return id < other.id;  // Compare based on the 'id'
  }
};

/**
 * This is the type of a single entry
 * in the log.
 */
struct Entry {
  ~Entry() { batch.reset(); }
  /** client ID the entry has been assigned to */
  ClientId client_id;

  /** GSN of the entry */
  uint64_t gsn;

  shared_ptr<Batch> batch;

  /** pointer to the next entry for this client */
  Entry* next;

  bool skip = false;

  /** pre-allocation time */
  uint64_t assignment_ts;

  /** The deadline by which the slot should be filled */
  uint64_t deadline = 0;
};

/**
 * Type of a single block which includes
 * the log entries of this block.
 */
using Block = std::array<Entry, slog::NUM_ENTRIES_PER_BLOCK>;
static_assert(sizeof(Block) % slog::HUGE_PAGE_SIZE == 0);
using Blocks = std::list<Block*>;

class Iterator {
 public:
  /**
   * Initialize the iterator.
   *
   * @param first_slot iterator to the first slot of the log
   * @param index  index of this iterator in the stride
   * @param stride stride with which to iterate the log
   * @param num_entries location holding the number of entries in the log
   */
  Iterator(Blocks::iterator first_slot, unsigned long index, unsigned long stride, unsigned long* num_entries);

  /**
   * Try to get the next entry from the log.
   *
   * @return pointer to the entry, if one was
   *         obtained, nullptr otherwise
   */
  Entry* NextEntry();

  bool NextEntryReady();

 private:
  /** index into the log */
  unsigned long entry_index_;

  /** stride with which to iterate the log */
  unsigned long stride_;

  /** number of entries in the global log */
  unsigned long* num_entries_;

  /** iterator to the current block */
  Blocks::iterator iterator_;

  /** index of the block in the log */
  unsigned long block_index_ = 0;

  /** iterator to the current entry */
  Entry* entry_ = nullptr;
};

/**
 * This class represents the clients logical position in the log.
 *
 * It may correspond to a physical position for SingleHome clients
 * or a logical position for MultiHome clients
 */
class Position {
 public:
  /**
   * Position default constructor
   *
   * @param log_identifier   Identifier of the log (for debugging)
   */
  Position(string log_identifier, RegionId local_region)
      : log_identifier_(log_identifier), client_batches_(0), local_region_(local_region) {};

  /**
   * Insert the given request into the log.
   *
   * @param batch   client batch of transactions
   * @param skip  the number of slots to skip
   */
  virtual void Insert(const shared_ptr<Batch>& batch, unsigned long skip) = 0;

  /**
   * Close this client's slots in the log.
   */
  virtual void Close() = 0;

  string log_identifier_;

  AsyncLog<std::pair<shared_ptr<Batch>, uint64_t>> client_batches_;

  // Local region determines whether the transaction belongs to the region or not
  RegionId local_region_;
};

class ClientState {
 public:
  ~ClientState() {
    if (client_pointer_ != nullptr) {
      client_pointer_->Close();
    }
  }
  Position* client_pointer_ = nullptr;
  uint64_t current_sent_request = 0;
};

class Log {
 public:
  virtual ~Log() = default;

  Log(const std::string& debug_tag, RegionId region, std::chrono::milliseconds epoch_duration)
      : debug_tag_(debug_tag), region_(region), epoch_duration_(epoch_duration) {}

  virtual void AddBatch(slog::internal::ForwardZiplogBatch* batch) = 0;
  virtual void AddEpoch(slog::internal::ZiplogRegionSlots* epoch, uint64_t epoch_start) = 0;
  virtual bool HasNextBatch() = 0;
  virtual std::tuple<SlotId, shared_ptr<Batch>, bool> NextBatch() = 0;
  virtual bool RemoveClient(slog::ClientId generator) = 0;
  virtual void Reset() = 0;

 public:
  uint32_t current_slot_ = 0;

 protected:
  std::string debug_tag_;
  RegionId region_;
  std::chrono::milliseconds epoch_duration_;
};

}  // namespace slog
