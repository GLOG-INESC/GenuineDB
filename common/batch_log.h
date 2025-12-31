#pragma once

#include <queue>
#include <unordered_map>

#include "common/async_log.h"
#include "common/types.h"
#include "proto/transaction.pb.h"

namespace slog {

using BatchPtr = std::unique_ptr<Batch>;

class BatchLog {
 public:
  BatchLog();

  void AddBatch(BatchPtr&& batch);
  void AddSlot(SlotId slot_id, BatchId batch_id, int replication_factor = 0, int64_t ordering_arrival_time = 0);
  void AckReplication(BatchId batch_id);

  bool HasNextBatch() const;
  std::pair<SlotId, BatchPtr> NextBatch();

  /* For debugging */
  size_t NumBufferedSlots() const { return slots_.NumBufferredItems(); }

  /* For debugging */
  size_t NumBufferedBatches() const { return batches_.size(); }

 private:
  void UpdateReadyBatches();

  AsyncLog<std::tuple<BatchId, int64_t, long>> slots_;
  std::unordered_map<BatchId, BatchPtr> batches_;
  std::unordered_map<BatchId, int> replication_;
  std::queue<std::pair<SlotId, std::tuple<BatchId, int64_t, long>>> ready_batches_;
};

}  // namespace slog