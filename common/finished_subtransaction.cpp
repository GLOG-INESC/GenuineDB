//
// Created by jrsoares on 18/12/25.
//

#include "finished_subtransaction.h"

#include <glog/logging.h>

#include "proto_utils.h"

namespace slog {

FinishedTransactionImpl::FinishedTransactionImpl(int num_replicas, int num_participants)
    : FinishedTransaction(num_replicas, num_participants) {}

bool FinishedTransactionImpl::AddSubTxn(EnvelopePtr&& new_req, MachineId machine_id) {
  DCHECK(new_req != nullptr);

  auto [quorum_it, inserted] = GetQuorum(machine_id);

  auto is_first_response = (txn_ == nullptr);
  // First response, use message as template
  if (is_first_response) {
    txn_.reset(new_req->mutable_request()->mutable_finished_subtxn()->release_txn());
    txn_->mutable_internal()->clear_involved_partitions();
    txn_->mutable_internal()->add_involved_partitions(GET_PARTITION_ID(machine_id));
  } else {
    auto subtxn = new_req->mutable_request()->mutable_finished_subtxn()->mutable_txn();

    slog::MergeTransaction(*txn_, *subtxn, false);

    if (inserted) {
      txn_->mutable_internal()->add_involved_partitions(GetPartitionId(machine_id));
    }
  }

  // Check if the quorum is done
  auto& quorum = quorum_it->second;
  if (!quorum.is_done()) {
    quorum.Inc();

    if (quorum.is_done()) {
      complete_responses_++;
    }
  }

  bool is_finished = complete_responses_ == num_participants_;

  // Update the set of events from the transaction to match the last adquired set
  // If the first response is enough to complete the set, then req_ already includes it

  if (is_finished && !is_first_response) {
    auto subtxn = new_req->mutable_request()->mutable_finished_subtxn()->mutable_txn();

    // The events of the transaction are the events of the last arriving subtxn
    txn_->mutable_internal()->clear_events();
    txn_->mutable_internal()->mutable_events()->CopyFrom(subtxn->mutable_internal()->events());

    if (subtxn->mutable_internal()->last_remote_read() != std::numeric_limits<uint32_t>::max()) {
      txn_->mutable_internal()->set_last_remote_read(subtxn->mutable_internal()->last_remote_read());
    }
  }

  return is_finished;
}

SpeculativeFinishedTransaction::SpeculativeFinishedTransaction(int num_replicas, int num_participants)
    : FinishedTransaction(num_replicas, num_participants) {};

bool SpeculativeFinishedTransaction::AddSubTxn(EnvelopePtr&& new_req, MachineId machine_id) {
  DCHECK(new_req != nullptr);


  auto executed_timestamp = new_req->mutable_request()->mutable_tiga_finished_subtxn()->timestamp();
  // Ignore the transaction, since we will be receiving another with higher timestamp
  if (txn_timestamp_ > executed_timestamp) {
    speculation_success_ = false;
    return false;
  }

  auto [quorum_it, inserted] = GetQuorum(machine_id);

  auto is_first_response = (txn_ == nullptr);

  if (is_first_response) {
    txn_.reset(new_req->mutable_request()->mutable_tiga_finished_subtxn()->mutable_finished_subtxn()->release_txn());
    txn_->mutable_internal()->clear_involved_partitions();
    txn_->mutable_internal()->add_involved_partitions(GET_PARTITION_ID(machine_id));

  } else if (txn_timestamp_ < executed_timestamp) {
    speculation_success_ = false;

    auto new_txn = new_req->mutable_request()->mutable_tiga_finished_subtxn()->mutable_finished_subtxn()->release_txn();
    new_txn->mutable_internal()->mutable_involved_partitions()->CopyFrom(txn_->mutable_internal()->involved_partitions());
    new_txn->mutable_internal()->add_involved_partitions(GET_PARTITION_ID(machine_id));

    txn_.reset(new_txn);
    for (auto& [_, quorum] : responses_) {
      quorum.reset();
    }
    complete_responses_ = 0;
  } else {
    auto subtxn = new_req->mutable_request()->mutable_tiga_finished_subtxn()->mutable_finished_subtxn()->mutable_txn();

    MergeTransaction(*txn_, *subtxn, false);

    if (inserted) {
      txn_->mutable_internal()->add_involved_partitions(GetPartitionId(machine_id));
    }
  }

  txn_timestamp_ = executed_timestamp;

  // Check if the quorum is done
  auto& quorum = quorum_it->second;
  if (!quorum.is_done()) {
    quorum.Inc();

    if (quorum.is_done()) {
      complete_responses_++;
    }
  }

  bool is_finished = complete_responses_ == num_participants_;

  // Update the set of events from the transaction to match the last adquired set
  // If the first response is enough to complete the set, then req_ already includes it

  if (is_finished && !is_first_response) {
    auto subtxn = new_req->mutable_request()->mutable_tiga_finished_subtxn()->mutable_finished_subtxn()->mutable_txn();

    // The events of the transaction are the events of the last arriving subtxn
    txn_->mutable_internal()->clear_events();
    txn_->mutable_internal()->mutable_events()->CopyFrom(subtxn->mutable_internal()->events());

    if (subtxn->mutable_internal()->last_remote_read() != std::numeric_limits<uint32_t>::max()) {
      txn_->mutable_internal()->set_last_remote_read(subtxn->mutable_internal()->last_remote_read());
    }
  }

  return is_finished;
}
}  // namespace slog
