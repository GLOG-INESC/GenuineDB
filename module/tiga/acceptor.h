#pragma once

#include <unordered_map>

#include "common/configuration.h"
#include "common/metrics.h"
#include "common/sharder.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/base/networked_module.h"
#include "proto/transaction.pb.h"

namespace tiga {

using slog::ConfigurationPtr;
using slog::EnvelopePtr;
using slog::MetricsRepositoryManagerPtr;
using slog::Transaction;
using slog::TxnId;

struct TigaLogEntry {

  TigaLogEntry(Transaction* txn) {
    txn_ = txn;
    agreement_complete_ = false;
    txn_id_ = txn->internal().id();
  }

  TigaLogEntry(std::vector<slog::Key> keys, TxnId txn_id) :
    keys_(std::move(keys)){
    txn_ = nullptr;
    agreement_complete_ = true;
    txn_id_ = txn_id;
  }

  Transaction* txn_;
  bool agreement_complete_;
  std::vector<slog::Key> keys_;
  TxnId txn_id_;

};

class TimestampAgreementInfo {
public:
  // When constructing the timestamp agreement before having received the transaction
  TimestampAgreementInfo(uint64_t timestamp) : entry_(nullptr) {
    proposed_ts_ = timestamp;
    set_proposed_ts_.insert(timestamp);
    num_acks_++;
  }

  // When constructing the timestamp agreement upon receiving the transaction
  TimestampAgreementInfo(const uint64_t timestamp, const int num_participants, TigaLogEntry* entry):
    entry_(entry) {
    num_participants_ = num_participants;
    proposed_ts_ = timestamp;
  }

  // To update the agreement info upon receiving the transaction
  void SetAgreementInfo(const uint64_t timestamp, const int num_participants, TigaLogEntry* entry) {
    proposed_ts_ = timestamp;
    num_participants_ = num_participants;
    entry_ = entry;
  }

  void AddProposedTs(const uint64_t timestamp) {
    set_proposed_ts_.insert(timestamp);
    num_acks_++;
    reached_agreement = (num_participants_ != 0 && num_acks_ == num_participants_);
  }

  bool ReachedAgreement() {
    return reached_agreement;
  }

  uint64_t AgreementResult() {
    CHECK(ReachedAgreement()) << "Reached Agreement should only be called after num_participants has been updated";
    auto max_ts = *set_proposed_ts_.rbegin();

    // We use 0 as a flag meaning that no further action is needed
    if (set_proposed_ts_.size() == 1 || max_ts == proposed_ts_) {
      return 0;
    }

    proposed_ts_ = max_ts;
    return max_ts;
  }

  TigaLogEntry* GetEntry() {
    TigaLogEntry* return_entry = entry_;
    entry_ = nullptr;
    return return_entry;
  }

private:
  std::set<uint64_t> set_proposed_ts_;
  int num_participants_ = 0;
  int num_acks_ = 0;
  uint64_t proposed_ts_;
  bool reached_agreement = false;
  TigaLogEntry* entry_;
};

struct PQEntry {
  uint64_t timestamp;
  TxnId txnId;
  TigaLogEntry* entry_;
};

struct PQEntryCompare {
  bool operator()(const PQEntry& a, const PQEntry& b) const {
    if (a.timestamp != b.timestamp)
      return a.timestamp < b.timestamp;
    return a.txnId < b.txnId;
  }
};

using PQSet = std::set<PQEntry, PQEntryCompare>;

class Acceptor : public slog::NetworkedModule {
 public:
  Acceptor(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
           const MetricsRepositoryManagerPtr& metrics_manager,
           std::chrono::milliseconds poll_timeout_ms = slog::kModuleTimeout, bool debug_mode = false);

  std::string name() const override { return "Acceptor"; }

 protected:
  void OnInternalRequestReceived(EnvelopePtr&& env) final;

 private:
  std::chrono::time_point<std::chrono::system_clock> GetTimestamp();
  void ProcessPreAccept(EnvelopePtr&& env);
  void ProcessTimestampAgreement(EnvelopePtr&& env);

  void UpdateDeadline(const slog::Key& key, uint64_t ts);
  void IssueAgreementRequest(TigaLogEntry* entry, uint64_t ts);

  void ReleaseTransactions();
  void PrintStats();

  std::string acceptor_id_debug_;

  // TODO - Update queue such that it correctly tie-breaks for requests with the same timestamp

  PQSet pq_;
  std::unordered_map<slog::Key, uint64_t> lastReleasedTxnDeadlines_;

  std::unordered_map<TxnId, TimestampAgreementInfo> txn_agreement_info_;

  bool release_thread_active_ = false;

  bool debug_mode_ = false;

  int debug_clock_advancement_ = 0;
};

}  // namespace janus