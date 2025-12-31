#include "module/tiga/coordinator.h"

#include <glog/logging.h>

#include <sstream>

#include "common/clock.h"
#include "common/constants.h"
#include "common/json_utils.h"
#include "common/proto_utils.h"

namespace tiga {

using slog::kForwarderChannel;
using slog::kSequencerChannel;
using slog::MakeMachineId;
using slog::UnpackMachineId;
using slog::internal::Envelope;
using slog::internal::Request;
using slog::internal::Response;
using slog::MetadataInitializer;
using slog::RegionId;
using slog::TransactionEvent;

/*
 * This module does not have anything to do with Forwarder. It just uses the Forwarder's stuff for convenience.
 */
Coordinator::Coordinator(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
                         const std::shared_ptr<slog::MetadataInitializer>& metadata_initializer,
                         const MetricsRepositoryManagerPtr& metrics_manager, std::chrono::milliseconds poll_timeout,
                         bool debug_mode)
    : NetworkedModule(context, config, config->forwarder_port(), kForwarderChannel, metrics_manager, poll_timeout,
                      true /* is_long_sender */),
      sharder_(slog::Sharder::MakeSharder(config)),
      metadata_initializer_(metadata_initializer),
      debug_mode_(debug_mode){

  std::stringstream coordinator_id_debug;
  coordinator_id_debug << "[COORDINATOR_" << (int)config->local_region() << "_" << (int)config->local_replica() << "_"  << (int)config->local_partition() << "] ";
  coordinator_id_debug_ = coordinator_id_debug.str();

  // TODO - Currently only supports already established network latencies
  CHECK(config->has_simulated_delays()) << "Tiga requires knowing the network latency between regions";

  for (RegionId reg = 0; reg < config->num_regions(); reg++){
    network_delays_[reg] = config->network_delays(reg);
  }
}

void Coordinator::OnInternalRequestReceived(EnvelopePtr&& env) {
  switch (env->request().type_case()) {
    case Request::kForwardTxn:
      StartNewTxn(move(env));
      break;
    case Request::kStats:
      PrintStats();
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(env->request().type_case(), Request) << "\"";
  }
}

void Coordinator::OnInternalResponseReceived(EnvelopePtr&& env) {
  switch (env->response().type_case()) {
    default:
      LOG(ERROR) << "Unexpected response type received: \"" << CASE_NAME(env->response().type_case(), Response) << "\"";
  }
}
std::chrono::time_point<std::chrono::system_clock> Coordinator::GetTimestamp() {
  // For testing purposes, to make timestamp acquisition stable
  if (debug_mode_) {
    return std::chrono::time_point<std::chrono::system_clock>{};
  }
  return std::chrono::high_resolution_clock::now();
}

void Coordinator::StartNewTxn(EnvelopePtr&& env) {

  auto txn = env->mutable_request()->mutable_forward_txn()->release_txn();
  auto txn_id = txn->internal().id();

  RECORD(txn->mutable_internal(), TransactionEvent::ENTER_COORDINATOR);

  // Figure out participating partitions and regions
  try {
    PopulateInvolvedMetadata(metadata_initializer_, *txn);
    PopulateInvolvedPartitions(sharder_, *txn);
    SetTransactionType(*txn);
    PopulateInvolvedRegions(*txn);
  } catch (std::invalid_argument& e) {
    LOG(ERROR) << "Only numeric keys are allowed while running in Simple Partitioning mode";
    return;
  }

  // Calculate the largest expected arrival time
  double largest_latency = 0;
  auto local_region = config()->local_region();

  for (auto & region : txn->mutable_internal()->involved_regions()) {
    largest_latency = std::max(largest_latency, network_delays_[local_region][region]);
  }

  uint64_t latency_nanoseconds = static_cast<uint64_t>(std::llround(largest_latency*1000000));

  auto now = GetTimestamp();

  auto timestamp = now + std::chrono::nanoseconds(latency_nanoseconds) += std::chrono::microseconds(config()->timestamp_buffer_us());

  auto num_involved_partitions = txn->internal().involved_partitions_size();

  // Collect participants

  RECORD(txn->mutable_internal(), TransactionEvent::EXIT_COORDINATOR);
  VLOG(2) << coordinator_id_debug_ << "Serializing Transaction " << txn_id << " to time " << timestamp.time_since_epoch().count();

  for (int i = 0; i < num_involved_partitions; ++i) {
    // Set txn proposed timestamp as largest expected arrival time + some delay Delta
    auto pre_accept_env = NewEnvelope();
    auto pre_accept = pre_accept_env->mutable_request()->mutable_tiga_pre_accept();
    pre_accept->set_proposed_timestamp(timestamp.time_since_epoch().count());

    auto p = txn->internal().involved_partitions(i);

    auto region = slog::GetRegionId(p);
    auto partition = slog::GetPartitionId(p);

    auto part_txn = GeneratePartitionedTxn(sharder_, txn, region, partition, i == num_involved_partitions-1);
    pre_accept->set_allocated_txn(part_txn);
    Send(std::move(pre_accept_env), p, kSequencerChannel);
  }
}

void Coordinator::PrintStats() {
  std::ostringstream oss;
  LOG(INFO) << "Coordinator state:\n" << oss.str();
}

}  // namespace janus