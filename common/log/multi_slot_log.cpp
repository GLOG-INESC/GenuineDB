#include "common/log/multi_slot_log.h"

#include <glog/logging.h>

#include <utility>

#include "common/metrics.h"

using std::make_pair;
using std::move;

namespace slog {

MultiHomePosition::MultiHomePosition(string log_identifier, RegionId local_region, NewMultiSlot* first_slot)
    : Position(std::move(log_identifier), local_region), first_entry_(first_slot) {};

void MultiHomePosition::Insert(const shared_ptr<Batch>& batch, unsigned long skip) {
  current_entry_ = current_entry_ == nullptr ? first_entry_ : current_entry_->next;
  CHECK(current_entry_ != nullptr) << log_identifier_ << "No next multi slot";

  bool is_noop = (batch->transactions_size() != 0 && batch->transactions(0).internal().noop());

  // Clock for statistics
  if (!is_noop) {
    while (batch->transactions_size() > 0) {
      auto txn = batch->mutable_transactions()->ReleaseLast();

      std::set<RegionId> regions;
      // Allocate the txn participating region
      for (auto involved_region : txn->internal().involved_regions()) {
        regions.insert(involved_region);
      }

      // If the txn is not involved in the current region, then ignore it
      if (regions.find(local_region_) == regions.end()){
        delete txn;
        continue;
      }

      // Find the furthest participating region of each transaction and add the transaction to its batch
      for (short slot_index = 0; slot_index < current_entry_->allocated_slots; slot_index++) {
        auto& [region, slot] = current_entry_->slots[slot_index];

        // If the region is found, insert the transaction in this batch
        if (regions.count(region)) {
          switch (slot->type) {
            case Slot::PENDING: {
              VLOG(4) << log_identifier_ << "Filling PENDING slot of client "
                      << CLIENT_ID_STR(std::get<0>(slot->pending));
              auto& pending_batch = std::get<2>(slot->pending);

              if (pending_batch == nullptr) {
                pending_batch = std::make_shared<Batch>();
                pending_batch->set_home(batch->home());
              }
              pending_batch->mutable_transactions()->AddAllocated(txn);
              break;
            }

            case Slot::ALOC: {
              VLOG(4) << log_identifier_ << "Filling ENTRY of client " << CLIENT_ID_STR(slot->slot->client_id)
                      << " with gsn " << slot->slot->gsn;
              batch->set_ordering_arrival(slot->slot->assignment_ts);
              if (slot->slot->batch == nullptr) {
                slot->slot->batch = std::make_shared<Batch>();
                slot->slot->batch->set_home(batch->home());
              }
              slot->slot->batch->mutable_transactions()->AddAllocated(txn);
              break;
            }
          }
          break;
        }
      }
    }
  }

  // Any slots that were not filled must be skipped

  for (short slot_index = 0; slot_index < current_entry_->allocated_slots; slot_index++) {
    auto& slot = current_entry_->slots[slot_index].second;
    switch (slot->type) {
      case Slot::PENDING:
        if (std::get<2>(slot->pending) == nullptr) {
          //VLOG(4) << log_identifier_ << "REMOVING UNUSED MULTI-SLOT pending of client " << CLIENT_ID_STR(std::get<0>(slot->pending));

          std::get<3>(slot->pending) = true;
        }
        break;
      case Slot::ALOC:
        if (slot->slot->batch == nullptr) {
          //VLOG(4) << log_identifier_ << "REMOVING UNUSED MULTI-SLOT  entry of client " << CLIENT_ID_STR(slot->slot->client_id) << " with gsn " << slot->slot->gsn;

          slot->slot->skip = true;
        }
        break;
    }
  }

  // make sure we close the required number of
  // slots for this request
  while (--skip) {
    current_entry_ = current_entry_->next;
    CHECK(current_entry_ != nullptr);

    for (short slot_index = 0; slot_index < current_entry_->allocated_slots; slot_index++) {
      auto& slot = current_entry_->slots[slot_index].second;
      switch (slot->type) {
        case Slot::PENDING:
          //VLOG(4) << log_identifier_ << "SKIPPING pending slot of client " << CLIENT_ID_STR(std::get<0>(slot->pending));

          std::get<3>(slot->pending) = true;

          break;
        case Slot::ALOC:
          //VLOG(4) << log_identifier_ << "SKIPPING entry of client " << CLIENT_ID_STR(slot->slot->client_id) << " with gsn " << slot->slot->gsn;
          slot->slot->skip = true;
          break;
      }
    }
  }
}

void MultiHomePosition::Close() {
  current_entry_ = (current_entry_ == nullptr) ? first_entry_ : current_entry_->next;

  // Skip all pending and allocated slots
  while (current_entry_ != nullptr) {
    for (short slot_index = 0; slot_index < current_entry_->allocated_slots; slot_index++) {
      auto slot = current_entry_->slots[slot_index].second;
      switch (slot->type) {
        case Slot::PENDING:
          std::get<3>(slot->pending) = true;
          break;
        case Slot::ALOC:
          slot->slot->skip = true;
          break;
      }
    }
    current_entry_ = current_entry_->next;
  }
}

Iterator::Iterator(Blocks::iterator first_slot, unsigned long index, unsigned long stride, unsigned long* num_entries)
    : entry_index_(index), stride_(stride), num_entries_(num_entries), iterator_(first_slot) {}

Entry* Iterator::NextEntry() {
  CHECK(entry_->batch != nullptr || entry_->skip);
  auto entry = entry_;
  entry_ = nullptr;
  return entry;
}

bool Iterator::NextEntryReady() {
  // if we don't have an entry under consideration
  // obtain the next one from the log
  if (entry_ == nullptr) {
    // if there are no entries available then return
    if (entry_index_ >= *num_entries_) {
      return false;
    }

    // advance the block iterator, if needed
    auto block = entry_index_ / slog::NUM_ENTRIES_PER_BLOCK;
    while (block > block_index_) {
      block_index_++;
      iterator_++;
    }

    // obtain the current entry
    auto index = entry_index_ % slog::NUM_ENTRIES_PER_BLOCK;
    entry_ = &(*iterator_)->operator[](index);
    entry_index_ += stride_;
  }

  // LOG(INFO) << "Deadline: " << entry_->deadline << " Now: "
  // <<(uint64_t)std::chrono::high_resolution_clock::now().time_since_epoch().count();
  return entry_->deadline <= (uint64_t)std::chrono::high_resolution_clock::now().time_since_epoch().count() &&
         (entry_->batch != nullptr || entry_->skip);
}

MultiSlotLog::MultiSlotLog(std::string debug_tag, RegionId local_region, std::chrono::milliseconds epoch_duration,
                           const ConfigurationPtr& config, std::map<slog::RegionId, std::array<double, MAX_NUM_REGIONS>> region_latencies, bool skip_slots)
    : Log(debug_tag, local_region, epoch_duration),
      skip_slots_(skip_slots),
      config_(config){
  if (region_latencies.empty()) {
    region_latencies_[0] = {0.1, 6, 30.5, 33, 33.5, 37.5, 74, 86.5};
    region_latencies_[1] = {6, 0.1, 25, 26, 38.5, 42.5, 66, 80};
    region_latencies_[2] = {30.5, 25, 0.1, 10, 68, 72.5, 53.5, 67};
    region_latencies_[3] = {33, 26, 10, 0.1, 63.5, 64, 47.5, 62};
    region_latencies_[4] = {33.5, 38.5, 68, 63.5, 0.1, 5.5, 101, 114.5};
    region_latencies_[5] = {37.5, 42.5, 72.5, 64, 5.5, 0.1, 104.5, 118};
    region_latencies_[6] = {74, 66, 53.5, 47.5, 101, 104.5, 0.1, 16};
    region_latencies_[7] = {86.5, 80, 67, 62, 114.5, 118, 16, 0.1};

  } else {
    region_latencies_ = region_latencies;
  }

  CHECK(!region_latencies_.empty()) << debug_tag << "No region latency was provided";
  CHECK(region_latencies_.find(local_region) != region_latencies_.end())
      << debug_tag << "No latencies found for local region " << (int)local_region;


  // Initialize log
  log_.emplace_back(static_cast<Block*>(AllocateHugePage(sizeof(Block))));
  block_ = log_.begin();
  entry_ = (*block_)->begin();

  log_iterator_ = new Iterator(log_.begin(), 0, 1, &num_entries_);

  for (auto& [r, latencies] : region_latencies_) {
    std::vector<std::pair<int, double>> ordered_latencies;

    for (auto i = 0; i < config_->num_regions(); i++){
         ordered_latencies.emplace_back(std::make_pair(i, latencies[i]));
   };


    std::sort(ordered_latencies.begin(), ordered_latencies.end(), [](const auto& a, const auto& b) {
      if (a.second != b.second) return a.second < b.second;  // sort by double
      return a.first < b.first;                              // tiebreak by char
    });

    auto& regs = furthest_regions_[r];
    regs.fill({-1, -1, false, false});

    auto curr_slot = 0;
    bool skip_entry = skip_slots;

    // Should skip entries before this one
    for (auto& [reg, latency] : ordered_latencies) {
      if (reg == local_region) {
        skip_entry = false;
      }

      // Dont allocate slots for the clients own region
      if (r == reg) {
        continue;
      }

      if (!skip_entry) {
        regs[curr_slot++] = {
            reg,
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::duration<double, std::milli>(latency))
                .count(),
            true, skip_entry};
      }
    }

    curr_slot = 0;
    LOG(INFO) << debug_tag << "Final Latencies " << (int)r;
    while (std::get<2>(regs[curr_slot])) {
      LOG(INFO) << debug_tag << "Region " << (int)std::get<0>(regs[curr_slot]) << " Latency: " << std::get<1>(regs[curr_slot]);
      curr_slot++;
    }
  }
}
void MultiSlotLog::Reset() {
  for (auto& block : log_) {
    for (auto& entry : *block) {
      entry.~Entry();
    }
    free(block);
  }

  for (auto& pending_slots : pending_slots_) {
    pending_slots.second.clear();
  }
  pending_slots_.clear();

  gsn_ = 0;

  entry_ = nullptr;

  log_.clear();
  log_.emplace_back(static_cast<Block*>(AllocateHugePage(sizeof(Block))));

  block_ = log_.begin();
  entry_ = (*block_)->begin();

  num_entries_ = 0;

  log_iterator_ = new Iterator(log_.begin(), 0, 1, &num_entries_);

  for (auto& client_state : client_state_) {
    client_state = nullptr;
  }

  registered_clients_ = 0;
}

MultiSlotLog::~MultiSlotLog() {
  Reset();
  delete log_iterator_;
}

void MultiSlotLog::AddBatch(slog::internal::ForwardZiplogBatch* batch_info) {
  ClientId client_id = batch_info->generator();

  auto client_pos = GetClientEntry(client_id);
  CHECK(client_pos < MAX_NUM_REGIONS * MAX_NUM_GEN_PER_REGION * MAX_NUM_ZIP_STUBS)
      << "CLient Id falls outside the scope";

  CHECK(client_state_[client_pos]->current_sent_request++ == batch_info->generator_position())
      << debug_tag_ << "Was expecting request " << to_string(client_state_[client_pos]->current_sent_request - 1)
      << " But got request " << to_string(batch_info->generator_position());

  client_state_[client_pos]->client_pointer_->Insert(
      std::shared_ptr<Batch>(batch_info->mutable_batch_data()->ReleaseLast()), batch_info->slots());
}

void MultiSlotLog::AddEpoch(slog::internal::ZiplogRegionSlots* epoch, uint64_t epoch_start) {
  uint64_t epoch_num_clients = epoch->num_clients();

  if (epoch_num_clients == 0) {
    LOG(ERROR) << debug_tag_ << "No clients for region " << region_;
    return;
  }

  auto start = std::chrono::high_resolution_clock::now();
  std::vector<std::pair<unsigned long, ClientInfo>> slots;
  std::unordered_map<ClientId, int> current_slot;
  current_slot.reserve(epoch_num_clients);
  slots.reserve(epoch_num_clients);

  auto const& epoch_duration_nano = std::chrono::duration_cast<std::chrono::nanoseconds>(epoch_duration_).count();

  // Add the slots to the respective clients
  std::set<ClientId> new_clients;
  for (uint64_t i = 0; i < epoch_num_clients; i++) {
    auto const& client = epoch->slots(i);
    auto client_slots = client.num_slots();

    if (!client_state_[GetClientEntry(client.client_id())]) {
      client_state_[GetClientEntry(client.client_id())] = std::make_shared<MultiSlotClientState>(client.client_type());
      registered_clients_++;
    }

    slots.emplace_back(client_slots, ClientInfo{client.client_id(), client.client_type(), client.client_region(),
                                                (epoch_duration_nano + client_slots - 1) / client_slots});

    //LOG(INFO) << "Client ID " << CLIENT_ID_STR(client.client_id()) << " slot distant: " << (epoch_duration_nano + client_slots - 1) / client_slots;
    current_slot[client.client_id()] = 0;
  }

  // Order in decreasing order
  std::sort(slots.begin(), slots.end(), std::greater<std::pair<unsigned long, ClientInfo>>());

  std::vector<std::pair<std::pair<unsigned long, ClientInfo>, std::shared_ptr<MultiSlotClientState>>> state;

  for (const auto& slot : slots) {
    state.emplace_back(slot, client_state_[GetClientEntry(slot.second.id)]);
  }

  // create the errors for the iteration
  auto error = slots.begin()->first / epoch_num_clients;
  std::vector<long long> errors(epoch_num_clients - 1, error);

  auto current_clock = std::chrono::high_resolution_clock::now();
  uint64_t current_time = current_clock.time_since_epoch().count();

  auto SetPendingBatch = [&](uint64_t deadline) {
    // Add pending Batches
    auto pending_batch = pending_slots_.begin();

    while (pending_batch != pending_slots_.end()) {
      if (pending_batch->first < deadline) {
        auto& pending_slot = pending_batch->second;

        // Transform these pending into allocated slots
        for (auto slot : pending_slot) {
          auto order_ts = std::get<4>(slot->pending);
          auto& entry = SetSingleSlot(std::get<0>(slot->pending));

          // Set the deadline expected to insert this slot
          entry.deadline = pending_batch->first;
          if (std::get<2>(slot->pending)) {
            entry.batch = std::move(std::get<2>(slot->pending));
            entry.batch->set_ordering_arrival(order_ts);
          }

          // reset client state pointer for later safe memory dealocation
          // std::get<1>(slot->pending).reset();

          entry.skip = std::get<3>(slot->pending);
          entry.assignment_ts = order_ts;
          slot->slot = &entry;

          slot->type = Slot::ALOC;
        }

        pending_batch = pending_slots_.erase(pending_batch);

      } else {
        break;
      }
    }
  };

  auto SetSlot = [&](const uint64_t epoch_start, const ClientInfo& client, int slot,
                     shared_ptr<MultiSlotClientState> client_state, const uint64_t order_time) {
    auto deadline = epoch_start + (slot + 1) * client.interval_between_slots;
    //LOG(INFO) << "Client " << client.id << " deadline: " << deadline;
    SetPendingBatch(deadline);

    // For each slot, we calculate the estimated arrival time to each region
    if (client_state->multi_slot_ == (*client_state->block_)->end()) {
      client_state->multi_slots_log_->emplace_back(
          static_cast<MultiSlotBlock*>(AllocateHugePage(sizeof(MultiSlotBlock))));
      client_state->block_++;
      client_state->multi_slot_ = (*client_state->block_)->begin();
    }

    auto& multi_slot = *client_state->multi_slot_++;

    auto& region_latencies = furthest_regions_[client.region];

    // auto assigning_region_latency = furthest_regions_[client.region]
    for (auto& [region, latency, is_set, skip] : region_latencies) {
      if (!is_set) {
        break;
      }

      auto& [reg, s] = multi_slot.slots[multi_slot.allocated_slots++];
      s = make_shared<Slot>(client.id, client_state, nullptr, skip, order_time);
      reg = region;
      pending_slots_[deadline + latency].push_back(s);
      //LOG(INFO) << "Pending with deadline" << deadline + latency;
    }

    // update the first and last entries for this client multi slot
    auto& [first, last] = client_state->multislots_;
    if (last != nullptr) [[likely]] {
      last->next = &multi_slot;
    } else {
      first = &multi_slot;
      client_state->client_pointer_ = new MultiHomePosition(debug_tag_, region_, first);
    }
    last = &multi_slot;
    client_state->num_multislots_++;

  };

  for (unsigned long i = 0; i < state[0].first.first; i++) {
    SetSlot(epoch_start, state[0].first.second, i, state[0].second, current_time);

    // update the errors for secondary client and yield if needed
    for (unsigned long j = 1; j < epoch_num_clients; j++) {
      if ((errors[j - 1] -= state[j].first.first) < 0) {
        errors[j - 1] += state[0].first.first;
        SetSlot(epoch_start, state[j].first.second, current_slot[state[j].first.second.id]++, state[j].second,
                current_time);
      }
    }
  }

  // Check if any pending are left behind

  SetPendingBatch(epoch_start + epoch_duration_nano);
  std::chrono::duration<double> processing_time = std::chrono::high_resolution_clock::now() - start;
  LOG(INFO) << "Finished allocation. Took " << processing_time.count() << " seconds";

  // send a warning if we did not finish in time for the next epoch
  if (std::chrono::high_resolution_clock::now().time_since_epoch().count() > epoch_start) {
    LOG(ERROR) << debug_tag_ << "Missed the deadline for allocating slots for this epoch";
  } else {
    LOG(INFO) << debug_tag_ << "Finished allocation";
  }
}

bool MultiSlotLog::HasNextBatch() { return log_iterator_->NextEntryReady(); }

std::tuple<SlotId, shared_ptr<Batch>, bool> MultiSlotLog::NextBatch() {
  CHECK(HasNextBatch()) << "NextBatch() was called when there is no ready batch";

  current_slot_++;

  auto entry = log_iterator_->NextEntry();

  return {entry->gsn, entry->batch, entry->skip};
}

bool MultiSlotLog::RemoveClient(ClientId generator) {
  // Remove the client if it exists
  if (client_state_[GetClientEntry(generator)]) {
    client_state_[GetClientEntry(generator)]->client_pointer_->Close();
    client_state_[GetClientEntry(generator)].reset();

    registered_clients_--;
  }

  return registered_clients_ == 0;
}


}  // namespace slog
