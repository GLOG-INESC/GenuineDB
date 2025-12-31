#include "module/forwarder.h"

#include <glog/logging.h>

#include <algorithm>
#include <unordered_map>

#include "common/clock.h"
#include "common/constants.h"
#include "common/json_utils.h"
#include "common/proto_utils.h"

using std::move;
using std::shared_ptr;
using std::sort;
using std::string;
using std::unique;

namespace slog {

using internal::Envelope;
using internal::Request;
using internal::Response;
namespace {

uint32_t ChooseRandomPartition(const Transaction& txn, std::mt19937& rg) {
  std::uniform_int_distribution<> idx(0, txn.internal().involved_partitions_size() - 1);
  return GET_PARTITION_ID(txn.internal().involved_partitions(idx(rg)));
}

}  // namespace

Forwarder::Forwarder(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
                     const shared_ptr<LookupMasterIndex>& lookup_master_index,
                     const std::shared_ptr<MetadataInitializer>& metadata_initializer,
                     const MetricsRepositoryManagerPtr& metrics_manager, std::chrono::milliseconds poll_timeout)
    : NetworkedModule(context, config, config->forwarder_port(), kForwarderChannel, metrics_manager, poll_timeout,
                      true /* is_long_sender */),
      sharder_(Sharder::MakeSharder(config)),
      lookup_master_index_(lookup_master_index),
      metadata_initializer_(metadata_initializer),
      partitioned_lookup_request_(config->num_partitions()),
      batch_size_(0),
      rg_(std::random_device{}()) {


  for (int i = 0; i < config->num_regions(); i++) {
    latencies_ns_.emplace_back(config->avg_latency_window_size());
  }

  if(config->has_simulated_delays()){
    auto network_delays = config->network_delays(config->local_region());

    for (int i = 0; i < config->num_regions(); i++){
      latencies_ns_[i].Add(network_delays[i]*1000000);
    }

  }
}

void Forwarder::Initialize() {
  if (config()->fs_latency_interval() > std::chrono::milliseconds(0) && !config()->has_simulated_delays()) {
    ScheduleNextLatencyProbe();
  }
}

void Forwarder::ScheduleNextLatencyProbe() {
  NewTimedCallback(config()->fs_latency_interval(), [this] {
    auto p = config()->leader_partition_for_multi_home_ordering();
    auto now = slog_clock::now().time_since_epoch().count();
    for (int r = 0; r < config()->num_regions(); r++) {
      auto env = NewEnvelope();
      auto ping = env->mutable_request()->mutable_ping();
      ping->set_src_time(now);
      ping->set_dst(r);
      Send(move(env), MakeMachineId(r, 0, p), kSequencerChannel);
    }
    ScheduleNextLatencyProbe();
  });
}

void Forwarder::OnInternalRequestReceived(EnvelopePtr&& env) {
  switch (env->request().type_case()) {
    case Request::kForwardTxn:
      ProcessForwardTxn(move(env));
      break;
    case Request::kZipBatch:
      ProcessForwardBatch(move(env));
      break;
    case Request::kLookupMaster:
      ProcessLookUpMasterRequest(move(env));
      break;
    case Request::kStats:
      ProcessStatsRequest(env->request().stats());
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(env->request().type_case(), Request) << "\"";
  }
}

void Forwarder::ProcessForwardTxn(EnvelopePtr&& env) {
  auto txn = env->mutable_request()->mutable_forward_txn()->mutable_txn();

  RECORD(txn->mutable_internal(), TransactionEvent::ENTER_FORWARDER);


  bool need_remote_lookup = false;
  for (auto& kv : *txn->mutable_keys()) {
    const auto& key = kv.key();
    auto value = kv.mutable_value_entry();

    // If there is only a single region, the metadata is irrelevant
    if (config()->num_regions() == 1) {
      value->mutable_metadata()->set_master(0);
      value->mutable_metadata()->set_counter(0);
    } else {

      Metadata metadata;
      if (lookup_master_index_->GetMasterMetadata(key, metadata)) {
        value->mutable_metadata()->set_master(metadata.master);
        value->mutable_metadata()->set_counter(metadata.counter);
      } else {
        auto new_metadata = metadata_initializer_->Compute(key);
        value->mutable_metadata()->set_master(new_metadata.master);
        value->mutable_metadata()->set_counter(new_metadata.counter);
      }

    }
  }

  try {
    PopulateInvolvedPartitions(sharder_, *txn);
  } catch (std::invalid_argument& e) {
    LOG(ERROR) << "Only numeric keys are allowed while running in Simple Partitioning mode";
    return;
  }

  // If there is no need to look master info from remote partitions,
  // forward the txn immediately
  if (!need_remote_lookup) {
    auto txn_type = SetTransactionType(*txn);
    VLOG(2) << "[FORWARDER] Determine txn " << TXN_ID_STR(txn->internal().id()) << " to be " << ENUM_NAME(txn_type, TransactionType)
            << " without remote master lookup";
    DCHECK(txn_type != TransactionType::UNKNOWN);
    Forward(move(env));
    return;
  }
  CHECK(false) << "System should never get here, havent updated the system to support new type of partition sets";

  VLOG(2) << "[FORWARDER] Remote master lookup needed to determine type of txn " << TXN_ID_STR(txn->internal().id());
  for (auto p : txn->internal().involved_partitions()) {
    if (p != config()->local_partition()) {
      partitioned_lookup_request_[p].mutable_request()->mutable_lookup_master()->add_txn_ids(txn->internal().id());
    }
  }
  pending_transactions_.insert_or_assign(txn->internal().id(), move(env));

  ++batch_size_;

  // If this is the first txn in the batch, schedule to send the batch at a later time
  if (batch_size_ == 1) {
    NewTimedCallback(config()->forwarder_batch_duration(), [this]() { SendLookupMasterRequestBatch(); });
    batch_starting_time_ = std::chrono::steady_clock::now();
  }
}

void Forwarder::ProcessForwardBatch(EnvelopePtr&& env) {

  auto txns = env->mutable_request()->mutable_zip_batch()->mutable_txns()->mutable_transactions();

  bool batch_need_remote_lookup = false;
  std::set<string> lookup_keys;
  int txn_lookups = 0;
  auto log_flag = 2;

  for (int i = 0; i < txns->size(); i++){

    bool txn_need_remote_lookup = false;

    auto txn = txns->Get(i);

    RECORD(txn.mutable_internal(), TransactionEvent::ENTER_FORWARDER);

    if (txn.internal().noop()){
      log_flag = 4;
      if (txns->size() > 1){
        LOG(ERROR) << "Noop came batched with other transactions";
      }
      continue;
    }


    for (auto& kv : *txn.mutable_keys()) {
      const auto& key = kv.key();
      auto value = kv.mutable_value_entry();

      // If there is only a single region, the metadata is irrelevant
      if (config()->num_regions() == 1) {
        value->mutable_metadata()->set_master(0);
        value->mutable_metadata()->set_counter(0);
      }
      Metadata metadata;
      if (lookup_master_index_->GetMasterMetadata(key, metadata)) {
        value->mutable_metadata()->set_master(metadata.master);
        value->mutable_metadata()->set_counter(metadata.counter);
      } else {
        auto new_metadata = metadata_initializer_->Compute(key);
        value->mutable_metadata()->set_master(new_metadata.master);
        value->mutable_metadata()->set_counter(new_metadata.counter);
      }
    }

    try {
      PopulateInvolvedPartitions(sharder_, txn);
    } catch (std::invalid_argument& e) {
      LOG(ERROR) << "Only numeric keys are allowed while running in Simple Partitioning mode";
      return;
    }
    auto txn_type = SetTransactionType(txn);
    VLOG(log_flag) << "[FORWARDER] Determine txn " << TXN_ID_STR(txn.internal().id()) << " to be " << ENUM_NAME(txn_type, TransactionType)
                   << " without remote master lookup";
    DCHECK(txn_type != TransactionType::UNKNOWN);

    (*txns)[i] = txn;
  }

  // If there is no need to look master info from remote partitions,
  // forward the txn immediately
  if (!batch_need_remote_lookup) {
    auto stub_id = env->mutable_request()->mutable_zip_batch()->client_id();
    auto slot = env->mutable_request()->mutable_zip_batch()->sent_batches();

    VLOG(log_flag) << "[FORWARDER] Batch " << slot << "/" << CLIENT_ID_STR(stub_id) << " does not need remote lookup";
    ForwardBatch(move(env));
    return;
  } else {

    pending_batches_.insert_or_assign(batch_lookups_++, PendingBatch(txn_lookups, move(env)));
  }


  ++batch_size_;

  // If this is the first txn in the batch, schedule to send the batch at a later time
  if (batch_size_ == 1) {
    NewTimedCallback(config()->forwarder_batch_duration(), [this]() { SendLookupMasterRequestBatch(); });
    batch_starting_time_ = std::chrono::steady_clock::now();
  }


}

void Forwarder::SendLookupMasterRequestBatch() {
  if (per_thread_metrics_repo != nullptr) {
    per_thread_metrics_repo->RecordForwarderBatch(batch_size_,
                                                  (std::chrono::steady_clock::now() - batch_starting_time_).count());
  }

  auto local_reg = config()->local_region();
  auto local_rep = config()->local_replica();
  auto num_partitions = config()->num_partitions();
  for (int part = 0; part < num_partitions; part++) {
    if (!partitioned_lookup_request_[part].request().lookup_master().txn_ids().empty()) {
      Send(partitioned_lookup_request_[part], MakeMachineId(local_reg, local_rep, part), kForwarderChannel);
      partitioned_lookup_request_[part].Clear();
    }
  }
  batch_size_ = 0;
}

void Forwarder::ProcessLookUpMasterRequest(EnvelopePtr&& env) {
  const auto& lookup_master = env->request().lookup_master();
  Envelope lookup_env;
  auto lookup_response = lookup_env.mutable_response()->mutable_lookup_master();
  auto results = lookup_response->mutable_lookup_results();

  lookup_response->mutable_txn_ids()->CopyFrom(lookup_master.txn_ids());
  for (int i = 0; i < lookup_master.keys_size(); i++) {
    const auto& key = lookup_master.keys(i);

    if (sharder_->is_local_key(key)) {
      if (Metadata metadata; lookup_master_index_->GetMasterMetadata(key, metadata)) {
        // If key exists, add the metadata of current key to the response
        auto key_metadata = results->Add();
        key_metadata->set_key(key);
        key_metadata->mutable_metadata()->set_master(metadata.master);
        key_metadata->mutable_metadata()->set_counter(metadata.counter);
      } else {
        // Otherwise, assign it to the default region for new key
        auto key_metadata = results->Add();
        key_metadata->set_key(key);
        auto new_metadata = metadata_initializer_->Compute(key);
        key_metadata->mutable_metadata()->set_master(new_metadata.master);
        key_metadata->mutable_metadata()->set_counter(new_metadata.counter);
      }
    }
  }
  Send(lookup_env, env->from(), kForwarderChannel);
}

void Forwarder::OnInternalResponseReceived(EnvelopePtr&& env) {
  switch (env->response().type_case()) {
    case Response::kLookupMaster:
      UpdateMasterInfo(move(env));
      break;
    case Response::kPong:
      UpdateLatency(move(env));
      break;
    default:
      LOG(ERROR) << "Unexpected response type received: \"" << CASE_NAME(env->response().type_case(), Response) << "\"";
  }
}

void Forwarder::UpdateMasterInfo(EnvelopePtr&& env) {
  const auto& lookup_master = env->response().lookup_master();
  std::unordered_map<std::string, int> index;
  for (int i = 0; i < lookup_master.lookup_results_size(); i++) {
    index[lookup_master.lookup_results(i).key()] = i;
  }

  for (auto txn_id : lookup_master.txn_ids()) {
    if (pending_transactions_.find(txn_id) != pending_transactions_.end()){
      // Transfer master info from the lookup response to its intended transaction
      auto& pending_env = pending_transactions_.find(txn_id)->second;
      auto txn = pending_env->mutable_request()->mutable_forward_txn()->mutable_txn();
      for (auto& kv : *txn->mutable_keys()) {
        if (!kv.value_entry().has_metadata()) {
          auto it = index.find(kv.key());
          if (it != index.end()) {
            const auto& result = lookup_master.lookup_results(it->second).metadata();
            kv.mutable_value_entry()->mutable_metadata()->CopyFrom(result);
          }
        }
      }

      auto txn_type = SetTransactionType(*txn);
      if (txn_type != TransactionType::UNKNOWN) {
        VLOG(2) << "Determine txn " << TXN_ID_STR(txn->internal().id()) << " to be "
                << ENUM_NAME(txn_type, TransactionType);
        Forward(move(pending_env));
        pending_transactions_.erase(txn_id);
      }
    } else if (pending_batches_txns_.find(txn_id) != pending_batches_txns_.end()){
      auto batch_id = pending_batches_txns_.find(txn_id)->second;
      auto it = pending_batches_.find(batch_id);
      if (it == pending_batches_.end()){
        pending_batches_txns_.erase(txn_id);
        continue;
      }

      auto& pending_batch = it->second.env;

      auto txns = pending_batch->mutable_request()->mutable_zip_batch()->mutable_txns()->mutable_transactions();

      for (int i = 0; i < txns->size(); i++) {
        auto txn = txns->Get(i);

        if (txn.internal().id() == txn_id){
          for (auto& kv : *txn.mutable_keys()) {
            if (!kv.value_entry().has_metadata()) {
              auto it = index.find(kv.key());
              if (it != index.end()) {
                const auto& result = lookup_master.lookup_results(it->second).metadata();
                kv.mutable_value_entry()->mutable_metadata()->CopyFrom(result);
              }
            }
          }
          auto txn_type = SetTransactionType(txn);
          (*txns)[i] = txn;
          if (txn_type != TransactionType::UNKNOWN) {
            VLOG(2) << "Determine txn " << TXN_ID_STR(txn.internal().id()) << " to be "
                    << ENUM_NAME(txn_type, TransactionType);
            if(--it->second.lookups == 0){
              ForwardBatch(move(pending_batch));
              pending_batches_.erase(batch_id);
            }
            pending_batches_txns_.erase(txn_id);
          }
          break;
        }
      }
    }
  }
}

void Forwarder::UpdateLatency(EnvelopePtr&& env) {
  auto now = slog_clock::now().time_since_epoch().count();
  const auto& pong = env->response().pong();
  auto& latency_ns = latencies_ns_[pong.dst()];

  latency_ns.Add(pong.dst_time() - pong.src_time());

  if (per_thread_metrics_repo != nullptr) {
    per_thread_metrics_repo->RecordForwSequLatency(pong.dst(), pong.src_time(), pong.dst_time(), now, latency_ns.avg());
  }
}

void Forwarder::Forward(EnvelopePtr&& env) {
  auto txn = env->mutable_request()->mutable_forward_txn()->mutable_txn();
  auto txn_internal = txn->mutable_internal();
  auto txn_id = txn_internal->id();
  auto txn_type = txn_internal->type();

  PopulateInvolvedRegions(*txn);
#ifdef SYSTEM_GLOG
  // If the transaction is single home and is from a local region, issue it directly to the sequencer
  if (txn_type == TransactionType::SINGLE_HOME && txn_internal->involved_regions(0) == config()->local_region()) {
#else
  if (txn_type == TransactionType::SINGLE_HOME) {
#endif
    // If this current region is its home, forward to the sequencer of the same machine
    // Otherwise, forward to the sequencer of a random machine in its home region
    auto home_region = txn->keys().begin()->value_entry().metadata().master();
    if (home_region == config()->local_region()) {
      VLOG(2) << "Current region is home of txn " << TXN_ID_STR(txn_id);

      RECORD(txn_internal, TransactionEvent::EXIT_FORWARDER_TO_SEQUENCER);

      Send(move(env), kSequencerChannel);
    } else {
      auto partition = ChooseRandomPartition(*txn, rg_);
      auto random_machine_in_home_region = MakeMachineId(home_region, 0, partition);

      VLOG(2) << "Forwarding txn " << TXN_ID_STR(txn_id) << " to its home region (reg: " << home_region
              << ", part: " << partition << ")";

      RECORD(txn_internal, TransactionEvent::EXIT_FORWARDER_TO_SEQUENCER);

      Send(*env, random_machine_in_home_region, kSequencerChannel);
    }
  } else {
    CHECK(txn_type == TransactionType::MULTI_HOME_OR_LOCK_ONLY ||
          (txn_type == TransactionType::SINGLE_HOME && txn_internal->involved_regions(0) != config()->local_region())) <<
        "Txn should either be multi home or be a single home of another region";
    RECORD(txn_internal, TransactionEvent::EXIT_FORWARDER_TO_SEQUENCER);

    if (config()->bypass_mh_orderer()) {
      VLOG(2) << "Txn " << TXN_ID_STR(txn_id) << " is a multi-home txn. Sending to the sequencer.";

      // Send the txn directly to sequencers of involved regions to generate lock-only txns
      auto part = config()->leader_partition_for_multi_home_ordering();

      if (config()->synchronized_batching()) {
        // If synchronized batching is on, compute the batching delay based on latency between the current
        // region to the involved regions
        std::vector<MachineId> destinations;
        int64_t max_avg_latency_ns = 0;
        for (auto reg : txn_internal->involved_regions()) {
          max_avg_latency_ns = std::max(max_avg_latency_ns, static_cast<int64_t>(latencies_ns_[reg].avg()));
          destinations.push_back(MakeMachineId(reg, 0, part));
        }

        auto now = std::chrono::high_resolution_clock::now();
        auto timestamp = now + std::chrono::nanoseconds(max_avg_latency_ns) +
                         std::chrono::microseconds(config()->timestamp_buffer_us());

        auto txn = env->mutable_request()->mutable_forward_txn()->mutable_txn();
        txn->mutable_internal()->set_timestamp(timestamp.time_since_epoch().count());
        txn->mutable_internal()->set_mh_depart_from_coordinator_time(now.time_since_epoch().count());
        Send(move(env), destinations, kSequencerChannel);
      } else {
        std::vector<MachineId> destinations;
        destinations.reserve(txn_internal->involved_regions_size());
        for (auto reg : txn_internal->involved_regions()) {
          destinations.push_back(MakeMachineId(reg, 0, part));
        }
        Send(move(env), destinations, kSequencerChannel);
      }
    } else {
      VLOG(2) << "Txn " << TXN_ID_STR(txn_id) << " is a multi-home txn. Sending to the orderer.";

      auto local_region = config()->local_region();

#ifdef SYSTEM_GLOG
      auto multi_home_channel = kGLOGStubChannel;
#else
      auto multi_home_channel = kMultiHomeOrdererChannel;
#endif
      // Send the txn to the orderer to form a global order
      if (config()->shrink_mh_orderer() || config()->shrink_stubs()) {
        auto dest = MakeMachineId(local_region, 0, config()->leader_partition_for_multi_home_ordering());
        Send(move(env), dest, multi_home_channel);
      } else {
        Send(move(env), multi_home_channel);
      }
    }
  }
}

EnvelopePtr Forwarder::NewBatch(uint64_t slots, ClientId client_id, uint64_t sent_batches){
  auto batch_env = NewEnvelope();
  auto batch = batch_env->mutable_request()->mutable_zip_batch();
  batch->set_slots(slots);
  batch->set_client_id(client_id);
  batch->set_sent_batches(sent_batches);
  return batch_env;
}

void Forwarder::ForwardBatch(EnvelopePtr&& env) {

  auto txns = env->mutable_request()->mutable_zip_batch()->mutable_txns()->mutable_transactions();
  auto client_type = env->mutable_request()->mutable_zip_batch()->client_type();
  auto num_batched_txn = txns->size();

  // Back a new Batch message per Region
  std::unordered_map<RegionId , EnvelopePtr> batches;

  if (client_type == SINGLE_HOME){
    batches.insert({config()->local_region(), move(NewBatch(env->mutable_request()->mutable_zip_batch()->slots(),
                                          env->mutable_request()->mutable_zip_batch()->client_id(),
                                          env->mutable_request()->mutable_zip_batch()->sent_batches()))});
  } else {
    for (RegionId region = 0; region < config()->num_regions(); region++){
      batches.insert({region, move(NewBatch(env->mutable_request()->mutable_zip_batch()->slots(),
                                                    env->mutable_request()->mutable_zip_batch()->client_id(),
                                            env->mutable_request()->mutable_zip_batch()->sent_batches()))});
    }
  }


  for (int i = 0; i < num_batched_txn; i++) {
    auto txn = txns->ReleaseLast();
    auto txn_internal = txn->mutable_internal();
    auto txn_id = txn_internal->id();
    auto txn_type = txn_internal->type();
    RECORD(txn_internal, TransactionEvent::EXIT_FORWARDER_TO_SEQUENCER);

    // TODO - Find a better way to do Noops
    if (txn_internal->noop()){
      if (client_type == MULTI_HOME_OR_LOCK_ONLY){
        for (auto & batch : batches){

          auto new_txn = batch.second->mutable_request()->mutable_zip_batch()->mutable_txns()->mutable_transactions()->Add();
          new_txn->CopyFrom(*txn);
        }
      } else {
        auto new_txn = batches[config()->local_region()]->mutable_request()->mutable_zip_batch()->mutable_txns()->mutable_transactions()->Add();
        new_txn->CopyFrom(*txn);
      }

      continue;
    }

    PopulateInvolvedRegions(*txn);
    if (txn_type == TransactionType::SINGLE_HOME) {
      auto home_region = txn->keys().begin()->value_entry().metadata().master();
      auto new_txn = batches.find(home_region)->second->mutable_request()->mutable_zip_batch()->mutable_txns()->mutable_transactions()->Add();
      new_txn->CopyFrom(*txn);
    } else if (txn_type == TransactionType::MULTI_HOME_OR_LOCK_ONLY) {

      for (auto region : txn_internal->involved_regions()){
        auto new_txn = batches.find(region)->second->mutable_request()->mutable_zip_batch()->mutable_txns()->mutable_transactions()->Add();
        new_txn->CopyFrom(*txn);
      }
    }

    delete txn;
  }

  for (auto & batch : batches){
    // For transactions to the local region, send directly to this servers sequencer channel
    if (batch.first == config()->local_region()){
      Send(move(batch.second), kSequencerChannel);
    }
    else {
      auto remote_machine_id = MakeMachineId(batch.first, config()->local_replica(), config()->local_partition());
      Send(move(batch.second), remote_machine_id, kSequencerChannel);
    }
  }

}

/**
 * {
 *    forw_batch_size:    int,
 *    forw_pending_txns:  [uint64],
 *    forw_num_pending_txns: int
 * }
 */
void Forwarder::ProcessStatsRequest(const internal::StatsRequest& stats_request) {
  using rapidjson::StringRef;

  int level = stats_request.level();

  rapidjson::Document stats;
  stats.SetObject();
  auto& alloc = stats.GetAllocator();

  stats.AddMember(StringRef(FORW_LATENCIES_NS),
                  ToJsonArray(
                      pending_transactions_, [](const auto& item) { return item.first; }, alloc),
                  alloc);

  stats.AddMember(StringRef(FORW_BATCH_SIZE), batch_size_, alloc);
  stats.AddMember(StringRef(FORW_NUM_PENDING_TXNS), pending_transactions_.size(), alloc);
  if (level > 0) {
    stats.AddMember(StringRef(FORW_PENDING_TXNS),
                    ToJsonArray(
                        pending_transactions_, [](const auto& item) { return item.first; }, alloc),
                    alloc);
  }

  // Write JSON object to a buffer and send back to the server
  rapidjson::StringBuffer buf;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
  stats.Accept(writer);

  auto env = NewEnvelope();
  env->mutable_response()->mutable_stats()->set_id(stats_request.id());
  env->mutable_response()->mutable_stats()->set_stats_json(buf.GetString());
  Send(move(env), kServerChannel);
}  // namespace slog

}  // namespace slog