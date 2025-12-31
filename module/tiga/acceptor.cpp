#include "module/tiga/acceptor.h"

#include <glog/logging.h>

#include <unordered_map>

#include "common/clock.h"
#include "common/constants.h"
#include "common/json_utils.h"
#include "common/proto_utils.h"
#include "common/types.h"
#include "common/bitmap.h"

namespace tiga {

using slog::kForwarderChannel;
using slog::kSchedulerChannel;
using slog::internal::Envelope;
using slog::internal::Request;
using slog::internal::Response;
using slog::TransactionEvent;

/*
 * This module does not have anything to do with Sequencer. It just uses the Sequencer's stuff for convenience.
 */
Acceptor::Acceptor(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
                   const MetricsRepositoryManagerPtr& metrics_manager, std::chrono::milliseconds poll_timeout, bool debug_mode)
    : NetworkedModule(context, config, config->sequencer_port(), slog::kSequencerChannel, metrics_manager, poll_timeout,
                      true /* is_long_sender */), debug_mode_(debug_mode) {

  std::stringstream acceptor_id_debug;
  acceptor_id_debug << "[ACCEPTOR_" << (int)config->local_region() << "_" << (int)config->local_replica() << "_"  << (int)config->local_partition() << "] ";
  acceptor_id_debug_ = acceptor_id_debug.str();

  lastReleasedTxnDeadlines_.reserve(125000000);
  // TODO: Update size of lastReleasedTxnDeadlines_
}

std::chrono::time_point<std::chrono::system_clock> Acceptor::GetTimestamp() {
  // For testing purposes, to make timestamp acquisition stable
  if (debug_mode_) {
    return std::chrono::time_point<std::chrono::system_clock>{std::chrono::seconds(1 + debug_clock_advancement_++)};
  }
  return std::chrono::high_resolution_clock::now();
}

void Acceptor::OnInternalRequestReceived(EnvelopePtr&& env) {
  switch (env->request().type_case()) {
    case Request::kTigaPreAccept:
      ProcessPreAccept(move(env));
      break;
    case Request::kTimestampAgreement:
      ProcessTimestampAgreement(move(env));
      break;
    case Request::kStats:
      PrintStats();
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(env->request().type_case(), Request) << "\"";
  }
}

void Acceptor::ProcessPreAccept(EnvelopePtr&& env) {

  auto txn = env->mutable_request()->mutable_tiga_pre_accept()->release_txn();
  auto proposed_ts = env->mutable_request()->mutable_tiga_pre_accept()->proposed_timestamp();

  // Check if the txn conflicts with any previously released txn

  bool requires_update = false;

  uint64_t max_ts = 0;

  RECORD(txn->mutable_internal(), TransactionEvent::ENTER_ACCEPTOR);
  for (auto const & key : *txn->mutable_keys()) {

    auto key_last_released = lastReleasedTxnDeadlines_.find(key.key());

    if (key_last_released != lastReleasedTxnDeadlines_.end()) {
      max_ts = std::max(max_ts, key_last_released->second);
      requires_update |= (key_last_released->second >= proposed_ts);
    }
  }

  // If requires update, update the txn time and insert it into the queue
  if (requires_update) {
    VLOG(2) << acceptor_id_debug_ << " Proposing new timestamp for Transaction " << txn->mutable_internal()->id() << " to time " << proposed_ts;

    proposed_ts = GetTimestamp().time_since_epoch().count();
    CHECK(max_ts < proposed_ts) << "New timestamp continues to be smaller - This should never happen";
  }

  // Finally, add transaction to queue
  VLOG(2) << acceptor_id_debug_ << " Adding Transaction " << txn->mutable_internal()->id() << " to queue with time " << proposed_ts;

  auto [pq_it, existed] = pq_.emplace(PQEntry{proposed_ts, txn->mutable_internal()->id(), new TigaLogEntry(txn)});
  CHECK(existed) << "Txn with that ID already existed";

  // If the transaction is single-home, it has already reached agreement by default
  if (txn->internal().type() == slog::SINGLE_HOME) {
    pq_it->entry_->agreement_complete_ = true;
  }

  if (!release_thread_active_) {
    release_thread_active_ = true;
    ReleaseTransactions();
  }
}


void Acceptor::ProcessTimestampAgreement(EnvelopePtr&& env) {


  auto ts_agreement = env->mutable_request()->mutable_timestamp_agreement();
  auto txn_id = ts_agreement->txn_id();
  auto proposed_ts = ts_agreement->proposed_ts();

  auto [it, inserted] = txn_agreement_info_.try_emplace(
    txn_id,
    TimestampAgreementInfo(proposed_ts)
  );

  if (!inserted) {
    it->second.AddProposedTs(proposed_ts);
  }

  // Check if it has reached agreement
  if (it->second.ReachedAgreement()) {

    auto final_ts = it->second.AgreementResult();

    // If resulting agreement has returned anything other than 0, then this leader speculated badly
    // Reinsert the transaction into que queue
    if (final_ts) {
      auto [pq_it, existed] = pq_.emplace(PQEntry{final_ts, txn_id, it->second.GetEntry() });
      CHECK(existed) << "Txn with that ID already existed";
      pq_it->entry_->agreement_complete_ = true;

      if (!release_thread_active_) {
        release_thread_active_ = true;
        NewTimedCallback(std::chrono::microseconds(0), [this] { ReleaseTransactions(); });
      }
    } else {
      auto spec_env = NewEnvelope();
      auto spec_confirmation = spec_env->mutable_request()->mutable_spec_confirmation();
      spec_confirmation->set_txn_id(txn_id);
      // Inform the Scheduler of the correct placement
      spec_confirmation->set_spec_confirmation(true);
      Send(std::move(spec_env), kSchedulerChannel);
    }

    txn_agreement_info_.erase(txn_id);
  }

}

void Acceptor::UpdateDeadline(const slog::Key& key, const uint64_t ts) {

  if (lastReleasedTxnDeadlines_[key] < ts) {
    lastReleasedTxnDeadlines_[key] = ts;
  }
  /*
  auto key_last_released = lastReleasedTxnDeadlines_.find(key);

  if (key_last_released != lastReleasedTxnDeadlines_.end()) {

    CHECK(key_last_released->second < ts) << "Should never release a key with lower timestamp than actually released (" << ts << " vs last released: " << key_last_released->second << ")";
  }
  lastReleasedTxnDeadlines_[key] = ts;
  */
}

void Acceptor::IssueAgreementRequest(TigaLogEntry* entry, uint64_t ts) {
  // Add information regarding txn agreement
  auto [it, inserted] = txn_agreement_info_.try_emplace(
    entry->txn_->internal().id(),
    TimestampAgreementInfo(ts, entry->txn_->internal().involved_partitions_size(), entry)
  );

  if (!inserted) {
    it->second.SetAgreementInfo(ts, entry->txn_->internal().involved_partitions_size(), entry);
  }

  // Issue the timestamp agreement to other leaders
  auto ts_agreement_env = NewEnvelope();
  auto ts_agreement_message = ts_agreement_env->mutable_request()->mutable_timestamp_agreement();

  ts_agreement_message->set_proposed_ts(ts);
  ts_agreement_message->set_txn_id(entry->txn_->internal().id());

  std::vector<slog::MachineId> involved_partitions;
  // TODO - Optimize such that it does not need to send itself the confirmation
  for (auto const & involved_part : entry->txn_->mutable_internal()->involved_partitions()) {
    involved_partitions.push_back(involved_part);
  }

  Send(std::move(ts_agreement_env), involved_partitions, slog::kSequencerChannel);
}

void Acceptor::ReleaseTransactions() {

  auto now = GetTimestamp().time_since_epoch().count();

  for (auto pq_iterator = pq_.begin(); pq_iterator != pq_.end();){
    auto & [ts, txn_id, entry] = *pq_iterator;

    if (ts > now) {
      // There are transactions in queue but cant process them right now, invoke future release transactions
      NewTimedCallback(std::chrono::microseconds(100), [this] { ReleaseTransactions(); });
      return;
    }

      auto env = NewEnvelope();
      auto spec_execution_request = env->mutable_request()->mutable_spec_execution();
      spec_execution_request->set_timestamp(ts);

      bool final_issue = false;

      if (entry->txn_ != nullptr) {

        auto txn = entry->txn_;

        RECORD(entry->txn_->mutable_internal(), TransactionEvent::EXIT_ACCEPTOR);
        // Save keyset in case we need to re-update the release set
        entry->keys_.reserve(txn->keys_size());
        for (auto const & key : *txn->mutable_keys()) {
          entry->keys_.push_back(key.key());
        }

        // Issue the transaction
        // TODO - Optimize such that issuing can be done earlier
        if (txn->mutable_internal()->type() == slog::SINGLE_HOME || entry->agreement_complete_) {
          final_issue = true;
          spec_execution_request->set_speculative(false);
        } else {
          IssueAgreementRequest(entry, ts);
          spec_execution_request->set_speculative(true);
        }

        spec_execution_request->set_allocated_txn(entry->txn_);
        entry->txn_ = nullptr;
      } else {
        final_issue = true;
        // Second time the transaction has passed, so its definitive result now
        // Issue with txn_id
        spec_execution_request->set_speculative(false);
        spec_execution_request->set_txn_id(entry->txn_id_);
      }

      for (auto const & key : entry->keys_) {
        if (lastReleasedTxnDeadlines_[key] < ts) {
          lastReleasedTxnDeadlines_[key] = ts;
        }
      }

    Send(std::move(env), kSchedulerChannel);
      // Delete entry if its the final passing of the txn
      if (final_issue) {
        delete entry;
      }

    pq_iterator = pq_.erase(pq_iterator);
  }

  // No more transactions, set flag to stop release cycle and wait for more transactions
  release_thread_active_ = false;
}

void Acceptor::PrintStats() {
  std::ostringstream oss;

  LOG(INFO) << "Acceptor state:\n" << oss.str();
}

}  // namespace janus