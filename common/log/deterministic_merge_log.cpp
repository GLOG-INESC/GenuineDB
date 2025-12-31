#include "common/log/deterministic_merge_log.h"

#include <glog/logging.h>

#include <utility>

#include "common/metrics.h"

using std::make_pair;
using std::move;

namespace slog {

SingleHomePosition::SingleHomePosition(string log_identifier, RegionId local_region, Entry* first_entry) :
                                                         Position(std::move(log_identifier), local_region), first_entry_(first_entry) {};

void SingleHomePosition::Insert(const shared_ptr<Batch>& batch, unsigned long skip) {
  // if the current entry is null, then
  // we must seek to the first entry of this client

  current_entry_ = current_entry_ == nullptr ? first_entry_ : current_entry_->next;

  CHECK(current_entry_ != nullptr) << " No entry to insert the request";

  bool is_noop = (batch->transactions_size() != 0 && batch->transactions(0).internal().noop());

  if (is_noop){
    current_entry_->skip = true;
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

    if (txns->empty()){
      current_entry_->skip = true;
    } else {
      batch->set_ordering_arrival(current_entry_->assignment_ts);

      current_entry_->batch = batch;
    }
  }

  // make sure we close the required number of
  // slots for this request

  while (--skip) {
    current_entry_ = current_entry_->next;
    CHECK(current_entry_ != nullptr);
    current_entry_->skip = true;
  }
}

void SingleHomePosition::Close() {
  current_entry_ = current_entry_ == nullptr ? first_entry_ : current_entry_->next;

  while (current_entry_ != nullptr) {
    current_entry_->skip = true;
    current_entry_ = current_entry_->next;
  }


}


DeterministicMergeLog::DeterministicMergeLog(std::string debug_tag, RegionId region, std::chrono::milliseconds epoch_duration)
    : Log(debug_tag, region, epoch_duration) {


  // Initialize log
  log_.emplace_back(static_cast<Block*>(AllocateHugePage(sizeof(Block))));
  block_ = log_.begin();
  entry_ = (*block_)->begin();

  log_iterator_ = new Iterator(log_.begin(), 0, 1, &num_entries_);

  // Calculate the regions further than the local one
}
void DeterministicMergeLog::Reset(){
  for (auto & block: log_){
    for (auto & entry : *block){
      entry.~Entry();
    }
    free(block);
  }

  for (auto & pending_slots : pending_slots_){
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


  for (auto & client_state : client_state_){
    client_state = nullptr;
  }

  registered_clients_ = 0;

}

DeterministicMergeLog::~DeterministicMergeLog(){
  Reset();
  delete log_iterator_;
}


void DeterministicMergeLog::AddBatch(slog::internal::ForwardZiplogBatch* batch_info) {
  ClientId client_id = batch_info->generator();

  auto client_pos = GetClientEntry(client_id);
  CHECK(client_pos < MAX_NUM_REGIONS*MAX_NUM_GEN_PER_REGION*MAX_NUM_ZIP_STUBS) << "CLient Id falls outside the scope";

  CHECK(client_state_[client_pos]->current_sent_request++ == batch_info->generator_position() ) << debug_tag_ << "Was expecting request " << to_string(client_state_[client_pos]->current_sent_request-1) << " But got request " << to_string(batch_info->generator_position());

  client_state_[client_pos]->client_pointer_->Insert(std::shared_ptr<Batch>(batch_info->mutable_batch_data()->ReleaseLast()), batch_info->slots());

}

void DeterministicMergeLog::AddEpoch(slog::internal::ZiplogRegionSlots* epoch, uint64_t epoch_start) {
  uint64_t epoch_num_clients = epoch->num_clients();

  if (epoch_num_clients == 0) {
    LOG(ERROR) << debug_tag_ << "No clients for region " << region_;
    return;
  }


  std::vector<std::pair<unsigned long, ClientInfo>> slots;
  std::unordered_map<ClientId, int> current_slot;
  current_slot.reserve(epoch_num_clients);
  slots.reserve(epoch_num_clients);

  auto const& epoch_duration_nano = std::chrono::duration_cast<std::chrono::nanoseconds>(epoch_duration_).count();

  std::set<ClientId> new_clients;
  for (uint64_t i = 0; i < epoch_num_clients; i++) {
    auto const& client = epoch->slots(i);
    auto client_slots = client.num_slots();

    if (!client_state_[GetClientEntry(client.client_id())]){
      client_state_[GetClientEntry(client.client_id())] = std::make_shared<DMClientState>();
      registered_clients_++;
    }

    slots.emplace_back(client_slots, ClientInfo{client.client_id(), client.client_type(), client.client_region(),
                                                (epoch_duration_nano + client_slots - 1) / client_slots});

    current_slot[client.client_id()] = 0;


  }

  // Order in decreasing order
  std::sort(slots.begin(), slots.end(), std::greater<std::pair<unsigned long, ClientInfo>>());

  std::vector<std::pair<std::pair<unsigned long, ClientInfo>, std::shared_ptr<DMClientState>>> state;

  for (const auto& slot : slots){
    state.emplace_back(slot,client_state_[GetClientEntry(slot.second.id)]);
  }

  // create the errors for the iteration
  auto error = slots.begin()->first / epoch_num_clients;
  std::vector<long long> errors(epoch_num_clients - 1, error);

  auto current_clock = std::chrono::high_resolution_clock::now();
  uint64_t current_time = current_clock.time_since_epoch().count();


  auto SetSlot = [&](const uint64_t epoch_start, const ClientInfo& client, int slot,
                     shared_ptr<DMClientState> client_state, const uint64_t order_time) {
    auto& entry = SetSingleSlot(client.id);

    entry.assignment_ts = order_time;

    // update the first and last entries for this client
    auto& [first, last] = client_state->entries_;
    if (last != nullptr) [[likely]] {
      last->next = &entry;
    } else {
      first = &entry;
      client_state->client_pointer_ = new SingleHomePosition(debug_tag_, region_, first);
    }
    last = &entry;
  };


  for (unsigned long i = 0; i < state[0].first.first; i++) {
    SetSlot(epoch_start, state[0].first.second, i, state[0].second, current_time);

    // update the errors for secondary client and yield if needed
    for (unsigned long j = 1; j < epoch_num_clients; j++) {
      if ((errors[j - 1] -= state[j].first.first) < 0) {
        errors[j - 1] += state[0].first.first;
        SetSlot(epoch_start, state[j].first.second, current_slot[state[j].first.second.id]++, state[j].second, current_time);
      }
    }
  }

  // send a warning if we did not finish in time for the next epoch
  if (std::chrono::high_resolution_clock::now().time_since_epoch().count() > epoch_start) {
    LOG(ERROR) << debug_tag_ << "Missed the deadline for allocating slots for this epoch";
  }

}

bool DeterministicMergeLog::HasNextBatch() { return log_iterator_->NextEntryReady(); }

std::tuple<SlotId, shared_ptr<Batch>, bool> DeterministicMergeLog::NextBatch() {
  CHECK(HasNextBatch()) << "NextBatch() was called when there is no ready batch";

  current_slot_++;

  auto entry = log_iterator_->NextEntry();

  return {entry->gsn, entry->batch, entry->skip};
}

bool DeterministicMergeLog::RemoveClient(ClientId generator) {

  // Remove the client if it exists
  if (client_state_[GetClientEntry(generator)]){
    client_state_[GetClientEntry(generator)]->client_pointer_->Close();
    client_state_[GetClientEntry(generator)].reset();

    registered_clients_--;
  }

  return registered_clients_ == 0;
}

}

