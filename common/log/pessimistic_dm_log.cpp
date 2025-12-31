#include "common/log/pessimistic_dm_log.h"

#include <glog/logging.h>

#include <utility>

#include "common/metrics.h"

using std::make_pair;
using std::move;

namespace slog {

FutureSlotClientPosition::FutureSlotClientPosition(string log_identifier, RegionId local_region, FutureSlot* first_slot)
    : Position(std::move(log_identifier), local_region), first_entry_(first_slot) {};

void FutureSlotClientPosition::Insert(const shared_ptr<Batch>& batch, unsigned long skip) {
  current_entry_ = current_entry_ == nullptr ? first_entry_ : current_entry_->next;
  CHECK(current_entry_ != nullptr) << log_identifier_ << "No next multi slot";

  bool is_noop = (batch->transactions_size() != 0 && batch->transactions(0).internal().noop());

  auto fill_slot = [](const std::shared_ptr<Slot>& slot, const shared_ptr<Batch>& batch, bool skip,
                      string log_identifier) {
    switch (slot->type) {
      case Slot::PENDING: {
        VLOG(3) << log_identifier << "Filling PENDING slot of client " << CLIENT_ID_STR(std::get<0>(slot->pending));

        if (skip){
          std::get<3>(slot->pending) = true;
        } else {
          std::get<2>(slot->pending) = batch;
        }

        break;
      }

      case Slot::ALOC: {
        VLOG(3) << log_identifier << "Filling ENTRY of client " << CLIENT_ID_STR(slot->slot->client_id) << " with gsn "
                << slot->slot->gsn;

        if (skip) {
          slot->slot->skip = true;
        } else {
          batch->set_ordering_arrival(slot->slot->assignment_ts);
          slot->slot->batch = batch;
        }
        break;
      }
    }
  };

  if (is_noop) {
    fill_slot(current_entry_->slot, batch, true, log_identifier_);
  } else {
    auto* txns = batch->mutable_transactions();

    int write_idx = 0;
    for (int read_idx = 0; read_idx < txns->size(); ++read_idx) {
      const auto& txn = txns->Get(read_idx);

      std::set<RegionId> regions;
      // Allocate the txn participating region
      bool is_local = false;
      for (auto involved_region : txn.internal().involved_regions()) {
        is_local |= (involved_region == local_region_);
      }

      if (is_local) {
        if (write_idx != read_idx) {
          txns->SwapElements(write_idx, read_idx);
        }
        ++write_idx;
      }
    }

    // Remove extra elements at the end
    while (txns->size() > write_idx) {
      txns->RemoveLast();
    }

    fill_slot(current_entry_->slot, batch, txns->empty(), log_identifier_);
  }

  // make sure we close the required number of
  // slots for this request
  while (--skip) {
    current_entry_ = current_entry_->next;
    CHECK(current_entry_ != nullptr);

    fill_slot(current_entry_->slot, batch, true, log_identifier_);
  }
}

void FutureSlotClientPosition::Close() {
  current_entry_ = (current_entry_ == nullptr) ? first_entry_ : current_entry_->next;

  // Skip all pending and allocated slots
  while (current_entry_ != nullptr) {
    auto slot = current_entry_->slot;
    switch (slot->type) {
      case Slot::PENDING:
        std::get<3>(slot->pending) = true;
        break;
      case Slot::ALOC:
        slot->slot->skip = true;
        break;
    }

    current_entry_ = current_entry_->next;
  }
}

PessimisticDMLog::PessimisticDMLog(std::string debug_tag, RegionId local_region,
                                   std::chrono::milliseconds epoch_duration, const ConfigurationPtr& config,
                                   std::map<slog::RegionId, std::array<double, MAX_NUM_REGIONS>> region_latencies)
    : Log(debug_tag, local_region, epoch_duration), config_(config) {
  if (region_latencies.empty()) {
    region_latencies[0] = {0.1, 6, 30.5, 33, 33.5, 37.5, 74, 86.5};
    region_latencies[1] = {6, 0.1, 25, 26, 38.5, 42.5, 66, 80};
    region_latencies[2] = {30.5, 25, 0.1, 10, 68, 72.5, 53.5, 67};
    region_latencies[3] = {33, 26, 10, 0.1, 63.5, 64, 47.5, 62};
    region_latencies[4] = {33.5, 38.5, 68, 63.5, 0.1, 5.5, 101, 114.5};
    region_latencies[5] = {37.5, 42.5, 72.5, 64, 5.5, 0.1, 104.5, 118};
    region_latencies[6] = {74, 66, 53.5, 47.5, 101, 104.5, 0.1, 16};
    region_latencies[7] = {86.5, 80, 67, 62, 114.5, 118, 16, 0.1};
  }

  CHECK(!region_latencies.empty()) << debug_tag << "No region latency was provided";
  CHECK(region_latencies.find(local_region) != region_latencies.end())
      << debug_tag << "No latencies found for local region " << (int)local_region;

  CHECK(region_latencies.size() >= config->num_regions())
      << "Mapping of latencies must be higher or equal to number of regions";

  furthest_region_latency_.fill(0);

  for (auto& [r, latencies] : region_latencies) {
    auto max_latency = *std::max_element(latencies.begin(), latencies.begin() + config->num_regions());

    furthest_region_latency_[r] =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::duration<double, std::milli>(max_latency))
            .count();
  }

  // Initialize log
  log_.emplace_back(static_cast<Block*>(AllocateHugePage(sizeof(Block))));
  block_ = log_.begin();
  entry_ = (*block_)->begin();

  log_iterator_ = new Iterator(log_.begin(), 0, 1, &num_entries_);
}

void PessimisticDMLog::Reset() {
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

PessimisticDMLog::~PessimisticDMLog() {
  Reset();
  delete log_iterator_;
}

void PessimisticDMLog::AddBatch(slog::internal::ForwardZiplogBatch* batch_info) {
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

void PessimisticDMLog::AddEpoch(slog::internal::ZiplogRegionSlots* epoch, uint64_t epoch_start) {
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
      client_state_[GetClientEntry(client.client_id())] = std::make_shared<FutureClientState>(client.client_type());
      registered_clients_++;
    }

    slots.emplace_back(client_slots, ClientInfo{client.client_id(), client.client_type(), client.client_region(),
                                                (epoch_duration_nano + client_slots - 1) / client_slots});

    // LOG(INFO) << "Client ID " << CLIENT_ID_STR(client.client_id()) << " slot distant: " << (epoch_duration_nano +
    // client_slots - 1) / client_slots;
    current_slot[client.client_id()] = 0;
  }

  // Order in decreasing order
  std::sort(slots.begin(), slots.end(), std::greater<std::pair<unsigned long, ClientInfo>>());

  std::vector<std::pair<std::pair<unsigned long, ClientInfo>, std::shared_ptr<FutureClientState>>> state;

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
                     shared_ptr<FutureClientState> client_state, const uint64_t order_time) {
    auto deadline = epoch_start + (slot + 1) * client.interval_between_slots;
    // LOG(INFO) << "Client " << client.id << " deadline: " << deadline;
    SetPendingBatch(deadline);

    if (client_state->future_slot_ == (*client_state->block_)->end()) {
      client_state->future_slots_log_->emplace_back(
          static_cast<FutureSlotBlock*>(AllocateHugePage(sizeof(FutureSlotBlock))));
      client_state->block_++;
      client_state->future_slot_ = (*client_state->block_)->begin();
    }

    auto& future_slot = *client_state->future_slot_++;

    auto& s = future_slot.slot;
    s = make_shared<Slot>(client.id, client_state, nullptr, false, order_time);

    pending_slots_[deadline + furthest_region_latency_[client.region]].push_back(s);

    // update the first and last entries for this client multi slot
    auto& [first, last] = client_state->future_slots_;
    if (last != nullptr) [[likely]] {
      last->next = &future_slot;
    } else {
      first = &future_slot;
      client_state->client_pointer_ = new FutureSlotClientPosition(debug_tag_, region_, first);
    }
    last = &future_slot;
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

bool PessimisticDMLog::HasNextBatch() { return log_iterator_->NextEntryReady(); }

std::tuple<SlotId, shared_ptr<Batch>, bool> PessimisticDMLog::NextBatch() {
  CHECK(HasNextBatch()) << "NextBatch() was called when there is no ready batch";

  current_slot_++;

  auto entry = log_iterator_->NextEntry();

  return {entry->gsn, entry->batch, entry->skip};
}

bool PessimisticDMLog::RemoveClient(ClientId generator) {
  // Remove the client if it exists
  if (client_state_[GetClientEntry(generator)]) {
    client_state_[GetClientEntry(generator)]->client_pointer_->Close();
    client_state_[GetClientEntry(generator)].reset();

    registered_clients_--;
  }

  return registered_clients_ == 0;
}

}  // namespace slog
