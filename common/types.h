#pragma once

#include <string>

#include "proto/transaction.pb.h"

namespace slog {

using Key = std::string;
using KeyRegion = std::string;
using Value = std::string;
using TxnId = uint64_t;
using BatchId = uint64_t;
using SlotId = uint32_t;
using Channel = uint64_t;
using MachineId = uint32_t;
using RegionId = uint8_t;
using ReplicaId = uint8_t;
using PartitionId = uint16_t;
using ClientStubId = uint32_t;
using ClientId = uint64_t;

using Deadline = uint64_t;
// Counter for client sent requests
using ClientRequestPosition = uint32_t;
using MultiSlotId = uint64_t;
using BatchPtr = std::unique_ptr<Batch>;

// Tiga Special Message
using TigaIssueTxnMessage = std::tuple<Transaction*, uint64_t, bool, bool>;
using TigaTxnExecutionResponse = std::pair<TxnId, bool>;

const int kRegionIdBits = sizeof(RegionId) * 8;
const int kReplicaIdBits = sizeof(ReplicaId) * 8;
const int kPartitionIdBits = sizeof(PartitionId) * 8;

const int kMachineIdBits = kRegionIdBits + kReplicaIdBits + kPartitionIdBits;

const int kClientSubIdBits = sizeof(ClientStubId) * 8;

const int kClientIdBits = kRegionIdBits + kReplicaIdBits + kClientSubIdBits;

inline MachineId MakeMachineId(RegionId region, ReplicaId replica, PartitionId partition) {
  return (static_cast<MachineId>(region) << (kReplicaIdBits + kPartitionIdBits)) |
         (static_cast<MachineId>(replica) << kPartitionIdBits) | (static_cast<MachineId>(partition));
}

inline ClientId MakeClientId(RegionId region, ReplicaId replica, PartitionId partition, ClientStubId client_id) {
  return (static_cast<ClientId>(region) << (kReplicaIdBits + kPartitionIdBits + kClientSubIdBits)) |
         (static_cast<ClientId>(replica) << (kPartitionIdBits + kClientSubIdBits)) |
         (static_cast<ClientId>(partition) << (kClientSubIdBits)) | (static_cast<ClientId>(client_id));
}

inline TxnId MakeTxnId(MachineId machine_id, int counter){
  return (static_cast<TxnId>(counter) << kMachineIdBits | machine_id);
}

// ClientID related getters
#define GET_CLIENT_REGION_ID(id) (((id) >> (kReplicaIdBits + kPartitionIdBits + kClientSubIdBits)) & ((1 << kRegionIdBits) - 1))
#define GET_CLIENT_REPLICA_ID(id) (((id) >> (kPartitionIdBits + kClientSubIdBits)) & ((1 << kReplicaIdBits) - 1))
#define GET_CLIENT_GEN_ID(id) (((id) >> (kClientSubIdBits)) & ((1 << kPartitionIdBits) - 1))
#define GET_CLIENT_ID(id) static_cast<uint32_t>((id) & ((1LL << kClientSubIdBits) - 1))

// MachineID related getters
#define GET_REGION_ID(id) (((id) >> (kReplicaIdBits + kPartitionIdBits)) & ((1 << kRegionIdBits) - 1))
#define GET_REPLICA_ID(id) (((id) >> kPartitionIdBits) & ((1 << kReplicaIdBits) - 1))
#define GET_PARTITION_ID(id) ((id) & ((1 << kPartitionIdBits) - 1))

// TxnID related getters
#define TXN_ID(machine_id, counter) ((counter << kMachineIdBits) | machine_id)
#define TXN_ID_GET_MACHINE_ID(txn_id) (txn_id & ((1LL << kMachineIdBits) - 1))
#define TXN_ID_GET_COUNTER(txn_id) (txn_id >> kMachineIdBits)

#define MACHINE_ID_STR(id)                                                                    \
  ("[" + std::to_string(GET_REGION_ID(id)) + "," + std::to_string(GET_REPLICA_ID(id)) + "," + \
   std::to_string(GET_PARTITION_ID(id)) + "]")

inline std::tuple<RegionId, ReplicaId, PartitionId> UnpackMachineId(MachineId id) {
  return std::make_tuple(GET_REGION_ID(id), GET_REPLICA_ID(id), GET_PARTITION_ID(id));
}

inline std::uint8_t GetRegionId(MachineId id) {
  return GET_REGION_ID(id);
}

inline std::uint16_t GetPartitionId(MachineId id) {
  return GET_PARTITION_ID(id);
}


#define TXN_ID_STR(id) \
  (std::to_string((id) >> kMachineIdBits) + "/" + MACHINE_ID_STR((id) & ((1LL << kMachineIdBits) - 1)))

inline std::string GetTxnIdStr(TxnId id) {
  return TXN_ID_STR(id);
}

#define CLIENT_ID_STR(id) \
  ("[" + std::to_string(GET_CLIENT_REGION_ID(id)) + "," + std::to_string(GET_CLIENT_REPLICA_ID(id)) + "," \
          +std::to_string(GET_CLIENT_GEN_ID(id)) + "," + std::to_string(GET_CLIENT_ID(id)) + "]")

struct Metadata {
  Metadata() = default;
  Metadata(const MasterMetadata& metadata) : master(metadata.master()), counter(metadata.counter()) {}
  Metadata(uint32_t m, uint32_t c = 0) : master(m), counter(c) {}
  void operator=(const MasterMetadata& metadata) {
    master = metadata.master();
    counter = metadata.counter();
  }

  uint32_t master = 0;
  uint32_t counter = 0;
};

struct Record {
  Record(const std::string& v, uint32_t m = 0, uint32_t c = 0) : metadata_(m, c) { SetValue(v); }

  Record(const Record& other) {
    SetValue(other.data_.get(), other.size_);
    SetMetadata(other.metadata_);
  }

  Record& operator=(const Record& other) {
    Record tmp(other);
    data_.swap(tmp.data_);
    std::swap(size_, tmp.size_);
    std::swap(metadata_, tmp.metadata_);
    return *this;
  }

  void SetMetadata(const Metadata& metadata) { metadata_ = metadata; }

  void SetValue(const std::string& v) { SetValue(v.data(), v.size()); }

  void SetValue(const char* data, size_t size) {
    size_ = size;
    data_.reset(new char[size_]);
    memcpy(data_.get(), data, size_);
  }

  std::string to_string() const {
    if (data_ == nullptr) {
      return "";
    }
    return std::string(data_.get(), size_);
  }

  Record() = default;

  const Metadata& metadata() const { return metadata_; }
  char* data() { return data_.get(); }
  size_t size() { return size_; }

 private:
  Metadata metadata_;
  std::unique_ptr<char[]> data_;
  size_t size_ = 0;
};

enum class LockMode { UNLOCKED, READ, WRITE };
enum class AcquireLocksResult { ACQUIRED, WAITING, ABORT };

inline KeyRegion MakeKeyRegion(const Key& key, uint32_t master) {
  std::string new_key;
  auto master_str = std::to_string(master);
  new_key.reserve(key.length() + master_str.length() + 1);
  new_key += key;
  new_key += ":";
  new_key += master_str;
  return new_key;
}

}  // namespace slog

namespace janus {

using TxnIdAndPartitionsBitmap = std::pair<slog::TxnId, uint64_t>;

}  // namespace janus