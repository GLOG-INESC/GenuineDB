#pragma once


#include <queue>
#include <unordered_map>
#include <list>

#include <glog/logging.h>

#include "common/constants.h"
#include "common/types.h"
#include "common/log/log.h"
#include "common/async_log.h"
#include "common/metrics.h"
#include "common/memory_allocation_utils.h"

namespace slog {

/**
 * This class represents a Single Home client's
 * position inside the log.
 *
 * This position interacts directly with the physical Log
 */
class SingleHomePosition : public Position{

 public:

  /**
     * Initialize a position in the log.
     *
     * @param first_entry First entry of the client in the log
   */
  SingleHomePosition(string log_identifier, RegionId local_region, Entry* first_entry);

  /**
     * Insert the given request into the log.
     *
     * @param batch   client batch of transactions
     * @param skip  the number of slots to skip
     *
     * @return GSN of the log entry
   */
  void Insert(const shared_ptr<Batch>& batch, unsigned long skip) override;

  /**
     * Close this client's slots in the log.
   */
  void Close() override;

 private:

  /** pointer to the current entry in the log */
  Entry* current_entry_ = nullptr;

  /** location of the pointer to the first entry */
  Entry* first_entry_;
};

class DMClientState : public ClientState {

 public:
  std::pair<Entry*, Entry*> entries_ = {nullptr, nullptr};

};


/**
 * Pre-ordered Log with multiple slots
 *
 * Interleaving protocol is based on ...
 *
 * */
class DeterministicMergeLog : public Log{
 public:
  DeterministicMergeLog(std::string debug_tag, RegionId region, std::chrono::milliseconds epoch_duration);
  ~DeterministicMergeLog();
  void AddBatch(slog::internal::ForwardZiplogBatch* batch_info) override;
  void AddEpoch(slog::internal::ZiplogRegionSlots* epoch, uint64_t epoch_start) override;
  bool HasNextBatch() override;
  std::tuple<SlotId, shared_ptr<Batch>, bool> NextBatch() override;
  bool RemoveClient(ClientId generator) override;
  void Reset() override;

  static inline uint64_t GetClientEntry(const ClientId client_id){
    return GET_CLIENT_REGION_ID(client_id) * MAX_NUM_GEN_PER_REGION * MAX_NUM_ZIP_STUBS
           + GET_CLIENT_GEN_ID(client_id) * MAX_NUM_ZIP_STUBS
           + GET_CLIENT_ID(client_id);
  }

  inline Entry& SetSingleSlot(const ClientId& client) {
    // if we have reached the end of the block then
    // allocate a new block and advance the iterators
    if (entry_ == (*block_)->end()) {
      log_.emplace_back(static_cast<Block*>(AllocateHugePage(sizeof(Block))));
      block_++;
      entry_ = (*block_)->begin();
    }

    auto& entry = *entry_++;
    entry.gsn = gsn_++;
    entry.client_id = client;

    num_entries_++;

    return entry;
 }

 // For testing purposes
 Blocks* GetLog() { return &log_; }

 unsigned long GetNumEntries() { return num_entries_; }

 protected:

  Blocks log_;

  unsigned long num_entries_ = 0;

  /** MultiSlots of all clients */
  std::array<std::shared_ptr<DMClientState>, MAX_NUM_REGIONS*MAX_NUM_GEN_PER_REGION*MAX_NUM_ZIP_STUBS> client_state_;

  /** iterator to the current entry */
  Entry* entry_ = nullptr;

  /** iterator to the last block */
  Blocks::iterator block_;

  unsigned long gsn_ = 0;

  /** Iterator to process the log */
  Iterator* log_iterator_;

  std::map<Deadline, std::vector<ClientId>> pending_slots_;

  uint64_t registered_clients_ = 0;
};

}  // namespace slog