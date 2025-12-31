//
// Created by jrsoares on 15/12/25.
//

#pragma once
#include "proto/transaction.pb.h"

namespace tiga {

using slog::Transaction;

struct KeyOperation {

  KeyOperation(slog::Key key, slog::KeyType type) {
    key_ = key;
    type_ = type;
  }
  slog::Key key_;
  slog::KeyType type_;
};

struct TigaTxnHolder {
  TigaTxnHolder(Transaction* txn, uint64_t spec_timestamp) : txn_(txn),  txn_id_(txn->mutable_internal()->id()), spec_timestamp_(spec_timestamp) {
    for (const auto& key : *txn->mutable_keys()) {
      keys_.emplace_back(key.key(), key.value_entry().type());
    }
  }


  Transaction* txn_;
  TxnId txn_id_;
  std::vector<KeyOperation> keys_;
  uint64_t spec_timestamp_;

  bool reached_agreement_ = false;
  bool completed_execution_ = false;
  bool issued_for_execution_ = false;
  bool ignore_result = false;

  int worker_ = -1;
};

}  // namespace tiga