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

#include "common/log/multi_slot_log.h"

namespace slog {

/**
 * This class represents the physical entries
 * corresponding to a logical slot
 */
struct FutureSlot {
  /** pointer to the next MultiSlot for this client */
  FutureSlot* next;

  std::shared_ptr<Slot> slot;
};

using FutureSlotBlock = std::array<FutureSlot, slog::NUM_ENTRIES_PER_BLOCK>;
static_assert(sizeof(FutureSlotBlock) % slog::HUGE_PAGE_SIZE == 0);

using FutureSlotBlocks = std::list<FutureSlotBlock*>;

class FutureSlotClientPosition : public Position {
 public:
  /**
   * Initialize the position in the logical log
   *
   * @param first_slot First entry in the logical slots
   */

  FutureSlotClientPosition(string log_identifier, RegionId local_region, FutureSlot* first_slot);

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
  FutureSlot* current_entry_ = nullptr;

  /** location of the pointer to the first entry */
  FutureSlot* first_entry_;
};

class FutureClientState : public ClientState {

 public:
  FutureClientState(TransactionType client_type) {
    client_type_ = client_type;

    future_slots_log_ = new FutureSlotBlocks();
    future_slots_log_->emplace_back(static_cast<FutureSlotBlock*>(AllocateHugePage(sizeof(FutureSlotBlock))));
    block_ = future_slots_log_->begin();
    future_slot_ = (*block_)->begin();
    future_slots_ = {nullptr, nullptr};
  };

  ~FutureClientState() {
    // Close all already allocated entries (both pending and allocated)
    client_pointer_->Close();

    for (auto& block : *future_slots_log_){
      for (auto& future_slot : *block){
        future_slot.slot.reset();
      }
      std::free(block);
    }
    delete future_slots_log_;

  }

  FutureSlotBlocks* future_slots_log_;

  Position* client_pointer_;

  TransactionType client_type_;

  /** iterator to the current Multi Slot */
  FutureSlot* future_slot_ = nullptr;

  /** iterator to the last block of the multi slot log */
  FutureSlotBlocks::iterator block_;

  unsigned long num_multislots_ = 0;

  // First and last multislot entry of the client
  std::pair<FutureSlot*, FutureSlot*> future_slots_;

  // Debug variable to check if we are getting requests in the correct order
  uint64_t current_sent_request = 0;
};

/**
 * Pre-ordered Log with multiple slots
 *
 * Interleaving protocol is based on ...
 *
 * */
class PessimisticDMLog : public Log {
 public:
  PessimisticDMLog(
      std::string debug_tag, RegionId local_region, std::chrono::milliseconds epoch_duration, const ConfigurationPtr& config,
      std::map<slog::RegionId, std::array<double, MAX_NUM_REGIONS>> region_latencies = std::map<slog::RegionId, std::array<double, MAX_NUM_REGIONS>>());
  ~PessimisticDMLog();
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

  std::array<unsigned long, MAX_NUM_REGIONS> furthest_region_latency_;

  /** MultiSlots of all clients */
  std::array<std::shared_ptr<FutureClientState>, MAX_NUM_REGIONS * MAX_NUM_GEN_PER_REGION * MAX_NUM_ZIP_STUBS> client_state_;

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