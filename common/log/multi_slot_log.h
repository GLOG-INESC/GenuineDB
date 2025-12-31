#pragma once

#include <glog/logging.h>

#include <list>
#include <queue>
#include <unordered_map>

#include "common/async_log.h"
#include "common/constants.h"
#include "common/log/log.h"
#include "common/metrics.h"
#include "common/types.h"
#include "common/memory_allocation_utils.h"

namespace slog {


/**
 * This class represents the slot abstraction.
 * This slot can either be:
 *      An Entry of the log;
 *      A deadline (if not yet assigned)
 */

using PendingEntry = std::tuple<ClientId, shared_ptr<ClientState>, shared_ptr<Batch>, bool, uint64_t>;
struct Slot {
  enum { PENDING, ALOC } type;
  union {
    PendingEntry pending;
    Entry* slot;
  };

  Slot(ClientId clientId, shared_ptr<ClientState> clientState, Batch* batchPtr, bool skip, uint64_t order_arrival)
      : type(PENDING), pending(clientId, clientState, move(batchPtr), skip, order_arrival) {};

  Slot(Entry* entry) : type(ALOC), slot(entry) {}

  ~Slot() {
    switch (type) {
      case PENDING:
        pending.~PendingEntry();
        break;
      case ALOC:
        slot = nullptr;
        break;
    }
  }
};

/**
 * This class represents the physical entries
 * corresponding to a logical slot
 */
struct NewMultiSlot {
  short allocated_slots = 0;

  /** pointer to the next MultiSlot for this client */
  NewMultiSlot* next;

  std::array<std::pair<RegionId, std::shared_ptr<Slot>>, MAX_NUM_REGIONS> slots;
};

using MultiSlotBlock = std::array<NewMultiSlot, slog::NUM_ENTRIES_PER_BLOCK>;
static_assert(sizeof(MultiSlotBlock) % slog::HUGE_PAGE_SIZE == 0);

using NewMultiSlotBlocks = std::list<MultiSlotBlock*>;

class MultiHomePosition : public Position {
 public:
  /**
   * Initialize the position in the logical log
   *
   * @param first_slot First entry in the logical slots
   */
  MultiHomePosition(string log_identifier, RegionId local_region, NewMultiSlot* first_slot);

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
  NewMultiSlot* current_entry_ = nullptr;

  /** location of the pointer to the first entry */
  NewMultiSlot* first_entry_;
};

class MultiSlotClientState : public ClientState {

 public:
  MultiSlotClientState(TransactionType client_type) {
    client_type_ = client_type;
    if (client_type == MULTI_HOME_OR_LOCK_ONLY) {
      multi_slots_log_ = new NewMultiSlotBlocks();
      multi_slots_log_->emplace_back(static_cast<MultiSlotBlock*>(AllocateHugePage(sizeof(MultiSlotBlock))));
      block_ = multi_slots_log_->begin();
      multi_slot_ = (*block_)->begin();
      multislots_ = {nullptr, nullptr};
    } else {
      entries_ = {nullptr, nullptr};
    }
  }
  ~MultiSlotClientState() {
    // Close all already allocated entries (both pending and allocated)
    client_pointer_->Close();

    // If multi home, dealocate multi log log
    if (client_type_ == MULTI_HOME_OR_LOCK_ONLY) {
      for (auto& block : *multi_slots_log_) {
        for (auto& multi_slot : *block) {
          for (auto& slot : multi_slot.slots) {
            slot.second.reset();
          }
        }

        std::free(block);
      }
      delete multi_slots_log_;
    }
  }

  NewMultiSlotBlocks* multi_slots_log_;

  TransactionType client_type_;

  /** iterator to the current Multi Slot */
  NewMultiSlot* multi_slot_ = nullptr;

  /** iterator to the last block of the multi slot log */
  NewMultiSlotBlocks::iterator block_;

  unsigned long num_multislots_ = 0;

  union {
    // First and last slot entry of the client
    std::pair<Entry*, Entry*> entries_;

    // First and last multislot entry of the client
    std::pair<NewMultiSlot*, NewMultiSlot*> multislots_;
  };

};

/**
 * Pre-ordered Log with multiple slots
 *
 * Interleaving protocol is based on ...
 *
 * */
class MultiSlotLog : public Log {
 public:
  MultiSlotLog(
      std::string debug_tag, RegionId local_region, std::chrono::milliseconds epoch_duration, const ConfigurationPtr& config,
      std::map<slog::RegionId, std::array<double, MAX_NUM_REGIONS>> region_latencies = std::map<slog::RegionId, std::array<double, MAX_NUM_REGIONS>>(),
      bool skip_slots = true);
  ~MultiSlotLog();
  void AddBatch(slog::internal::ForwardZiplogBatch* batch_info) override;
  void AddEpoch(slog::internal::ZiplogRegionSlots* epoch, uint64_t epoch_start) override;
  bool HasNextBatch() override;
  std::tuple<SlotId, shared_ptr<Batch>, bool> NextBatch() override;
  bool RemoveClient(ClientId generator) override;
  void Reset() override;

  static inline uint64_t GetClientEntry(const ClientId client_id) {
    return GET_CLIENT_REGION_ID(client_id) * MAX_NUM_GEN_PER_REGION * MAX_NUM_ZIP_STUBS +
           GET_CLIENT_GEN_ID(client_id) * MAX_NUM_ZIP_STUBS + GET_CLIENT_ID(client_id);
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

 private:
  Blocks log_;

  unsigned long num_entries_ = 0;

  /** Propagation delay between regions, used to create the multiple slots */
  std::map<RegionId, std::array<double, MAX_NUM_REGIONS>> region_latencies_;

  std::array<std::array<std::tuple<RegionId, unsigned long, bool /*region is set*/, bool /* region to skip*/>,
                        MAX_NUM_REGIONS>,
             MAX_NUM_REGIONS>
      furthest_regions_;

  /** MultiSlots of all clients */
  std::array<std::shared_ptr<MultiSlotClientState>, MAX_NUM_REGIONS * MAX_NUM_GEN_PER_REGION * MAX_NUM_ZIP_STUBS> client_state_;

  // std::unordered_map<ClientId, ClientState> client_state_;

  /** iterator to the current entry */
  Entry* entry_ = nullptr;

  /** iterator to the last block */
  Blocks::iterator block_;

  unsigned long gsn_ = 0;

  /** Iterator to process the log */
  Iterator* log_iterator_;

  std::map<Deadline, std::vector<shared_ptr<Slot>>> pending_slots_;

  uint64_t registered_clients_ = 0;

  /** Flag representing whether unnecessary slots should be allocated or not */
  bool skip_slots_;

  ConfigurationPtr config_;

};

}  // namespace slog