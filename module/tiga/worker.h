#pragma once

#include <functional>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <zmq.hpp>

#include "../../storage/speculative_mem_only_storage.h"
#include "common/configuration.h"
#include "common/metrics.h"
#include "common/types.h"
#include "execution/execution.h"
#include "module/base/networked_module.h"
#include "module/scheduler_components/txn_holder.h"
#include "proto/internal.pb.h"
#include "proto/transaction.pb.h"

namespace tiga {

using slog::Broker;
using slog::EnvelopePtr;
using slog::Execution;
using slog::MetricsRepositoryManagerPtr;
using slog::SpeculativeMemOnlyStorage;
using slog::Transaction;
using slog::TxnId;

using RunId = pair<TxnId, bool>;

const std::string kSchedWorkerAddress = "inproc://sched_worker";

struct TransactionState {
  enum class Phase { READ_LOCAL_STORAGE, WAIT_REMOTE_READ, EXECUTE, FINISH };

  TransactionState(Transaction* txn, uint64_t timestamp, bool speculative_exec) : txn(txn), timestamp(timestamp), remote_reads_waiting_on(0), phase(Phase::READ_LOCAL_STORAGE), speculative_exec(speculative_exec) {}
  Transaction* txn;
  uint64_t timestamp;
  uint32_t remote_reads_waiting_on;
  Phase phase;
  bool speculative_exec;
};

/**
 * A worker executes and commits transactions. Every time it receives from
 * the scheduler a message pertaining to a transaction X, it will either
 * initializes the state for X if X is a new transaction or try to advance
 * X to the subsequent phases as much as possible.
 */
class Worker : public slog::NetworkedModule {
 public:
  Worker(int id, const std::shared_ptr<Broker>& broker, const std::shared_ptr<SpeculativeMemOnlyStorage>& storage,
         const MetricsRepositoryManagerPtr& metrics_manager,
         std::chrono::milliseconds poll_timeout_ms = slog::kModuleTimeout);

  std::string name() const override { return "Worker-" + std::to_string(channel()); }

 protected:
  void Initialize() final;
  /**
   * Applies remote read for transactions that are in the WAIT_REMOTE_READ phase.
   * When all remote reads are received, the transaction is moved to the EXECUTE phase.
   */
  void OnInternalRequestReceived(EnvelopePtr&& env) final;

  /**
   * Receives new transaction from the scheduler
   */
  bool OnCustomSocket() final;

 private:
  /**
   * Drives most of the phase transition of a transaction
   */
  void AdvanceTransaction(const RunId& txn_id);

  /**
   * Checks master metadata information and reads local data to the transaction
   * buffer, then broadcast local data to other partitions
   */
  void ReadLocalStorage(const RunId& txn_id);

  /**
   * Executes the code inside the transaction
   */
  void Execute(const RunId& txn_id);

  /**
   * Returns the result back to the scheduler and cleans up the transaction state
   */
  void Finish(const RunId& txn_id);

  void BroadcastReads(const RunId& txn_id);

  // Precondition: txn_id must exists in txn states table
  TransactionState& TxnState(const RunId& run_id);

  void StartRedirection(const RunId& run_id);
  void StopRedirection(const RunId& run_id);

  int id_;
  std::shared_ptr<SpeculativeMemOnlyStorage> storage_;
  std::unique_ptr<slog::SpeculativeStorageExecution> execution_;
  const slog::SharderPtr sharder_;

  std::map<RunId, TransactionState> txn_states_;
};

}  // namespace janus