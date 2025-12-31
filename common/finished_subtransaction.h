//
// Created by jrsoares on 18/12/25.
//
#pragma once

#include "proto/internal.pb.h"
#include "proto/transaction.pb.h"
#include "types.h"
#include "common/string_utils.h"

namespace slog {

using EnvelopePtr = std::unique_ptr<internal::Envelope>;

using ServerId = std::pair<RegionId, PartitionId>;
// Quorum function for each participant

class Quorum {
public:
  Quorum(int num_replicas) : num_replicas_(num_replicas), count_(0) {}

  void Inc() { count_++; }

  bool is_done() { return count_ >= (num_replicas_ + 1) / 2; }

  void reset() {
    count_ = 0;
  }
  std::string to_string() const {
    std::ostringstream oss;
    oss << "(" << count_ << "/" << num_replicas_ << ")";
    return oss.str();
  }

private:
  const int num_replicas_;
  int count_;
};

class FinishedTransaction {
public:
  virtual ~FinishedTransaction() = default;

  FinishedTransaction(int num_replicas, int num_participants) : num_replicas_(num_replicas), num_participants_(num_participants) {
  }

  virtual bool AddSubTxn(EnvelopePtr&& new_req, MachineId machine_id) = 0;
  Transaction* ReleaseTxn() {
    return txn_.release();
  }

  std::pair<std::map<ServerId, Quorum>::iterator, bool> GetQuorum(MachineId machine_id) {

    ServerId identifier = std::make_pair(GET_REGION_ID(machine_id), GET_PARTITION_ID(machine_id));
    auto [it, inserted] = responses_.try_emplace(identifier, num_replicas_);

    return responses_.try_emplace(identifier, num_replicas_);
  }

  bool SpeculationSuccess() { return speculation_success_; }

protected:
  std::map<ServerId, Quorum> responses_;
  int num_replicas_;
  std::unique_ptr<Transaction> txn_;
  int num_participants_;
  int complete_responses_ = 0;
  bool speculation_success_ = true;

};

class FinishedTransactionImpl : public FinishedTransaction {

public:
  FinishedTransactionImpl(int num_replicas, int num_participants);
  bool AddSubTxn(EnvelopePtr&& new_req, MachineId machine_id) final;
};

class SpeculativeFinishedTransaction : public FinishedTransaction {
public:
  SpeculativeFinishedTransaction(int num_replicas, int num_participants);

  bool AddSubTxn(EnvelopePtr&& new_req, MachineId machine_id) final;
private:
  uint64_t txn_timestamp_ = 0;
};


}  // namespace slog
