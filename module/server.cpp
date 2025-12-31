#include "module/server.h"

#include <memory>

#include "common/constants.h"
#include "common/json_utils.h"
#include "common/proto_utils.h"
#include "connection/zmq_utils.h"
#include "proto/internal.pb.h"

using std::move;
using std::string;

namespace slog {

namespace {

void PreprocessTxn(Transaction* txn) {
  txn->set_status(TransactionStatus::NOT_STARTED);
  for (auto& kv : *(txn->mutable_keys())) {
    kv.mutable_value_entry()->clear_metadata();
  }
  auto txn_internal = txn->mutable_internal();
  txn_internal->set_type(TransactionType::UNKNOWN);
  txn_internal->clear_involved_regions();
  txn_internal->clear_involved_partitions();
  txn_internal->clear_active_partitions();
  txn_internal->set_timestamp(0);
}
}  // namespace

Server::Server(const std::shared_ptr<Broker>& broker, const MetricsRepositoryManagerPtr& metrics_manager,
               std::chrono::milliseconds poll_timeout)
    : NetworkedModule(broker, kServerChannel, metrics_manager, poll_timeout),
      rate_limiter_(config()->tps_limit()),
      txn_id_counter_(0) {

  std::stringstream debug_tag;
  debug_tag << "[SERVER_" << (int)config()->local_region() << "_" << (int)config()->local_replica() << "_" << config()->local_partition() << "] ";
  debug_tag_ = debug_tag.str();

}

/***********************************************
                Initialization
***********************************************/

void Server::Initialize() {
  string endpoint = "tcp://*:" + std::to_string(config()->server_port());
  zmq::socket_t client_socket(*context(), ZMQ_ROUTER);
  client_socket.set(zmq::sockopt::rcvhwm, 0);
  client_socket.set(zmq::sockopt::sndhwm, 0);
  client_socket.bind(endpoint);

  LOG(INFO) << debug_tag_ << "Bound Server to: " << endpoint;

  // Tell other machines that the current one is online
  internal::Envelope env;
  env.mutable_request()->mutable_signal();
  for (MachineId m : config()->all_machine_ids()) {
    if (m != config()->local_machine_id()) {
      offline_machines_.insert(m);
      Send(env, m, kServerChannel);
    }
  }

  AddCustomSocket(move(client_socket));
}

/***********************************************
                  API Requests
***********************************************/

bool Server::OnCustomSocket() {
  auto& socket = GetCustomSocket(0);
  zmq::message_t identity_zmq;

  if (!socket.recv(identity_zmq, zmq::recv_flags::dontwait)) {
    return false;
  }
  if (!identity_zmq.more()) {
    LOG(ERROR) << debug_tag_ << "Invalid message from client: Only identity part is found";
    return false;
  }
  api::Request request;
  if (!RecvDeserializedProtoWithEmptyDelim(socket, request)) {
    LOG(ERROR) << debug_tag_ << "Invalid message from client: Body is not a proto";
    return false;
  }

  auto identity = std::string(static_cast<const char*>(identity_zmq.data()), identity_zmq.size());
  // While this is called txn id, we use it for any kind of request

  switch (request.type_case()) {
    case api::Request::kTxn: {
      auto txn_id = NextTxnId();
      auto res = pending_responses_.try_emplace(txn_id, identity, request.stream_id());
      DCHECK(res.second) << debug_tag_ << "Duplicate transaction id: " << txn_id;

      auto txn = request.mutable_txn()->release_txn();
      auto txn_internal = txn->mutable_internal();
      txn_internal->set_id(txn_id);
      txn_internal->set_coordinating_server(config()->local_machine_id());

      if (!rate_limiter_.Request()) {
        txn->set_status(TransactionStatus::ABORTED);
        txn->set_abort_code(AbortCode::RATE_LIMITED);
        SendTxnToClient(txn);
        break;
      }

      RECORD(txn_internal, TransactionEvent::ENTER_SERVER);

      if (txn->keys().empty()) {
        txn->set_status(TransactionStatus::ABORTED);
        txn->set_abort_reason("txn accesses no key");
      } else if (txn->has_remaster() && config()->num_regions() == 1) {
        txn->set_status(TransactionStatus::ABORTED);
        txn->set_abort_reason("remaster txn cannot be used when there is only a single region");
      }

      if (txn->status() == TransactionStatus::ABORTED) {
        SendTxnToClient(txn);
        break;
      }

      PreprocessTxn(txn);

      RECORD(txn_internal, TransactionEvent::EXIT_SERVER_TO_FORWARDER);

      // Send to forwarder
      auto env = NewEnvelope();
      env->mutable_request()->mutable_forward_txn()->set_allocated_txn(txn);
      Send(move(env), kForwarderChannel);
      break;
    }
    case api::Request::kBatch: {
      auto txns = new Batch();
      auto batch = request.mutable_batch()->release_batch();

      // Batch originating home
      txns->set_home(batch->home());

      int batch_size = batch->mutable_transactions()->size();
      bool noop = false;
      for (int i = 0; i < batch_size; i++){
        auto txn = batch->mutable_transactions()->ReleaseLast();

        auto txn_id = NextTxnId();
        // We use the txn id sent by the client to differentiate it and then define a unique txn id
        auto res = pending_responses_.try_emplace(txn_id, identity, txn->mutable_internal()->id());
        DCHECK(res.second) << debug_tag_ << "Duplicate transaction id: " << txn_id;

        auto txn_internal = txn->mutable_internal();

        auto log_flag = 2;
        if (txn_internal->noop()){
          noop = true;
          log_flag = 4;
        }
        VLOG(log_flag) << debug_tag_ << "Transaction " << txn->mutable_internal()->id() << "/" << CLIENT_ID_STR(txn->mutable_internal()->client_id()) << " entered server. Became Txn " << TXN_ID_STR(txn_id);


        txn_internal->set_id(txn_id);
        txn_internal->set_coordinating_server(config()->local_machine_id());
        if (!rate_limiter_.Request()) {
          txn->set_status(TransactionStatus::ABORTED);
          txn->set_abort_code(AbortCode::RATE_LIMITED);
          SendTxnToClient(txn);
          continue;
        }

        // Record the pending txn events that still did not have a txn_id
        RECORD_PENDING(txn_internal);
        RECORD(txn_internal, TransactionEvent::ENTER_SERVER);

        if (txn->keys().empty() && !txn->internal().noop()) {
          txn->set_status(TransactionStatus::ABORTED);
          txn->set_abort_reason("txn accesses no key");
        } else if (txn->has_remaster() && config()->num_regions() == 1) {
          txn->set_status(TransactionStatus::ABORTED);
          txn->set_abort_reason("remaster txn cannot be used when there is only a single region");
        }

        if (txn->status() == TransactionStatus::ABORTED) {
          SendTxnToClient(txn);
          continue;
        }
        PreprocessTxn(txn);
        RECORD(txn_internal, TransactionEvent::EXIT_SERVER_TO_FORWARDER);
        txns->mutable_transactions()->AddAllocated(txn);
      }

      // Send to forwarder
      auto env = NewEnvelope();
      env->mutable_request()->mutable_zip_batch()->set_allocated_txns(txns);
      env->mutable_request()->mutable_zip_batch()->set_slots(request.mutable_batch()->slots());
      env->mutable_request()->mutable_zip_batch()->set_client_id(request.mutable_batch()->client_id());
      env->mutable_request()->mutable_zip_batch()->set_sent_batches(request.mutable_batch()->sent_batches());
      env->mutable_request()->mutable_zip_batch()->set_client_type(request.mutable_batch()->client_type());
      auto log_flag = noop ? 4 : 3;
      VLOG(log_flag) << debug_tag_ << "Batch " << request.mutable_batch()->sent_batches() << "/" <<
                          CLIENT_ID_STR(request.mutable_batch()->client_id()) << " forwarded. Home: " << batch->home();
      Send(move(env), kForwarderChannel);
      break;
    }

    case api::Request::kStats: {
      auto txn_id = NextTxnId();
      auto res = pending_responses_.try_emplace(txn_id, identity, request.stream_id());
      DCHECK(res.second) << debug_tag_ << "Duplicate transaction id: " << txn_id;
      auto env = NewEnvelope();
      env->mutable_request()->mutable_stats()->set_id(txn_id);
      env->mutable_request()->mutable_stats()->set_level(request.stats().level());

      // Send to appropriate module based on provided information
      switch (request.stats().module()) {
        case ModuleId::SERVER:
          ProcessStatsRequest(env->request().stats());
          break;
        case ModuleId::FORWARDER:
          Send(move(env), kForwarderChannel);
          break;
        case ModuleId::MHORDERER:
          Send(move(env), kMultiHomeOrdererChannel);
          break;
        case ModuleId::SEQUENCER:
          Send(move(env), kSequencerChannel);
          break;
        case ModuleId::SCHEDULER:
          Send(move(env), kSchedulerChannel);
          break;
        default:
          LOG(ERROR) << debug_tag_ << "Invalid module for stats request";
          break;
      }
      break;
    }
    case api::Request::kMetrics: {
      auto txn_id = NextTxnId();
      auto res = pending_responses_.try_emplace(txn_id, identity, request.stream_id());
      DCHECK(res.second) << debug_tag_ << "Duplicate transaction id: " << txn_id;
      metrics_manager().AggregateAndFlushToDisk(request.metrics().prefix());
      api::Response response;
      response.mutable_metrics();
      SendResponseToClient(txn_id, std::move(response));
      break;
    }
    default:
      auto txn_id = NextTxnId();
      auto res = pending_responses_.try_emplace(txn_id, identity, request.stream_id());
      DCHECK(res.second) << debug_tag_ << "Duplicate transaction id: " << txn_id;
      pending_responses_.erase(txn_id);
      LOG(ERROR) << debug_tag_ << "Unexpected request type received: \"" << CASE_NAME(request.type_case(), api::Request) << "\"";
      break;
  }
  return true;
}

/***********************************************
              Internal Requests
***********************************************/

void Server::OnInternalRequestReceived(EnvelopePtr&& env) {
  switch (env->request().type_case()) {
    case internal::Request::kSignal: {
      LOG(INFO) << debug_tag_ << "Machine " << MACHINE_ID_STR(env->from()) << " is online";
      offline_machines_.erase(env->from());
      if (offline_machines_.empty()) {
        LOG(INFO) << debug_tag_ << "All machines are online";
      }
      break;
    }
    case internal::Request::kFinishedSubtxn:
      ProcessFinishedSubtxn(move(env));
      break;
    case internal::Request::kTigaFinishedSubtxn:
      ProcessFinishedSubtxn(move(env));
      break;
    default:
      LOG(ERROR) << debug_tag_ << "Unexpected request type received: \"" << CASE_NAME(env->request().type_case(), internal::Request)
                 << "\"";
  }
}

void Server::ProcessFinishedSubtxn(EnvelopePtr&& env) {
#ifdef SYSTEM_TIGA
  auto tiga_finished = env->mutable_request()->mutable_tiga_finished_subtxn();
  auto finished_subtxn  = tiga_finished->mutable_finished_subtxn();
#else
  auto finished_subtxn = env->mutable_request()->mutable_finished_subtxn();
#endif
  auto txn = finished_subtxn->mutable_txn();
  auto txn_internal = txn->mutable_internal();

  RECORD(txn_internal, TransactionEvent::RETURN_TO_SERVER);

  auto txn_id = txn_internal->id();
  if (pending_responses_.count(txn_id) == 0) {
    return;
  }

  auto reg = GET_REGION_ID(env->from());
  auto rep = GET_REPLICA_ID(env->from());
  auto part = GET_PARTITION_ID(env->from());

  auto num_involved = txn_internal->involved_partitions_size();

#ifdef FULL_EXEC
  // In full execution mode, we wait for one response from each partition
  auto involved_parts = std::set<PartitionId>();

  for (auto involved_part : txn_internal->involved_partitions()) {
    involved_parts.insert(GET_PARTITION_ID(involved_part));
  }
  num_involved = involved_parts.size();
#endif


  // Different systems require different handling of finished transactions
  if (finished_txns_.find(txn_id) == finished_txns_.end()) {
#ifdef SYSTEM_TIGA
    finished_txns_[txn_id] = std::make_unique<SpeculativeFinishedTransaction>(1, num_involved);
#else
    finished_txns_[txn_id] = std::make_unique<FinishedTransactionImpl>(1, num_involved);
#endif
  }

  VLOG(4) << debug_tag_ << "Got finished txn " << TXN_ID_STR(txn_id) << " from " << MACHINE_ID_STR(env->from()) << " which has " << num_involved << " involved partitions";
  if (finished_txns_[txn_id]->AddSubTxn(std::move(env), env->from())) {

#ifdef SYSTEM_TIGA
    if (per_thread_metrics_repo != nullptr) {
      per_thread_metrics_repo->RecordSpeculationSuccess(txn_id, finished_txns_[txn_id]->SpeculationSuccess());
    }
#endif
    auto last_machine_id = env->from();
    auto response_txn = finished_txns_[txn_id]->ReleaseTxn();
    response_txn->mutable_internal()->set_last_machine_id(last_machine_id);
    SendTxnToClient(response_txn);
    finished_txns_.erase(txn_id);
  }


}

void Server::ProcessStatsRequest(const internal::StatsRequest& stats_request) {
  using rapidjson::StringRef;

  int level = stats_request.level();

  rapidjson::Document stats;
  stats.SetObject();
  auto& alloc = stats.GetAllocator();

  // Add stats for current transactions in the system
  stats.AddMember(StringRef(TXN_ID_COUNTER), txn_id_counter_, alloc);
  stats.AddMember(StringRef(NUM_PENDING_RESPONSES), pending_responses_.size(), alloc);
  stats.AddMember(StringRef(NUM_PARTIALLY_FINISHED_TXNS), finished_txns_.size(), alloc);
  if (level >= 1) {
    stats.AddMember(StringRef(PENDING_RESPONSES),
                    ToJsonArrayOfKeyValue(
                        pending_responses_, [](const auto& resp) { return resp.stream_id; }, alloc),
                    alloc);

    stats.AddMember(StringRef(PARTIALLY_FINISHED_TXNS),
                    ToJsonArray(
                        finished_txns_, [](const auto& p) { return p.first; }, alloc),
                    alloc);
  }

  rapidjson::StringBuffer buf;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
  stats.Accept(writer);

  auto env = NewEnvelope();
  env->mutable_response()->mutable_stats()->set_id(stats_request.id());
  env->mutable_response()->mutable_stats()->set_stats_json(buf.GetString());
  OnInternalResponseReceived(move(env));
}

/***********************************************
              Internal Responses
***********************************************/

void Server::OnInternalResponseReceived(EnvelopePtr&& env) {
  if (env->response().type_case() != internal::Response::kStats) {
    LOG(ERROR) << debug_tag_ << "Unexpected response type received: \"" << CASE_NAME(env->response().type_case(), internal::Response)
               << "\"";
  }
  api::Response response;
  auto stats_response = response.mutable_stats();
  stats_response->set_allocated_stats_json(env->mutable_response()->mutable_stats()->release_stats_json());
  SendResponseToClient(env->response().stats().id(), move(response));
}

/***********************************************
                    Helpers
***********************************************/

void Server::SendTxnToClient(Transaction* txn) {
  RECORD(txn->mutable_internal(), TransactionEvent::EXIT_SERVER_TO_CLIENT);

  api::Response response;
  auto txn_response = response.mutable_txn();
  txn_response->set_allocated_txn(txn);
  SendResponseToClient(txn->internal().id(), move(response));
}

void Server::SendResponseToClient(TxnId txn_id, api::Response&& res) {
  auto it = pending_responses_.find(txn_id);
  if (it == pending_responses_.end()) {
    LOG(ERROR) << debug_tag_ << "Cannot find info to response back to client for txn: " << TXN_ID_STR(txn_id);
    return;
  }
  auto& socket = GetCustomSocket(0);


  auto log_flag = res.mutable_txn()->txn().internal().noop() ? 4 : 1;

  VLOG(log_flag) << debug_tag_ << "Responding to transaction {" << res.mutable_txn()->txn().internal().client_id() << ":" << it->second.stream_id << "}";
  // Stream id is for the client to match request/response
  res.set_stream_id(it->second.stream_id);
  // Send identity to the socket to select the client to response to
  socket.send(zmq::message_t(it->second.identity), zmq::send_flags::sndmore);
  // Send the actual message
  SendSerializedProtoWithEmptyDelim(socket, res);

  pending_responses_.erase(txn_id);
}

TxnId Server::NextTxnId() {
  txn_id_counter_++;
  return TXN_ID(config()->local_machine_id(), txn_id_counter_);
}

}  // namespace slog