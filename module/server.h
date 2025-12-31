#pragma once

#include <glog/logging.h>

#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include <zmq.hpp>

#include "common/configuration.h"
#include "common/rate_limiter.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/base/networked_module.h"

#include "proto/api.pb.h"
#include "common/finished_subtransaction.h"

namespace slog {

/**
 * A Server serves external requests from the clients.
 *
 * INPUT:  External TransactionRequest
 *
 * OUTPUT: For external TransactionRequest, it forwards the txn internally
 *         to appropriate modules and waits for internal responses before
 *         responding back to the client with an external TransactionResponse.
 */

class Server : public NetworkedModule {
 public:
  Server(const std::shared_ptr<Broker>& broker, const MetricsRepositoryManagerPtr& metrics_manager,
         std::chrono::milliseconds poll_timeout = kModuleTimeout);

  std::string name() const override { return "Server"; }

 protected:
  void Initialize() final;

  /**
   * After a transaction is processed by different partitions, each
   * involving partition will send a sub-transaction with the processing
   * result to the coordinating server. The coordinating server will be
   * in charge of merging these sub-transactions and responding back to
   * the client.
   */
  void OnInternalRequestReceived(EnvelopePtr&& env) final;

  void OnInternalResponseReceived(EnvelopePtr&& env) final;

  bool OnCustomSocket() final;

 private:
  void ProcessFinishedSubtxn(EnvelopePtr&& req);
  void ProcessStatsRequest(const internal::StatsRequest& stats_request);

  void SendTxnToClient(Transaction* txn);
  void SendResponseToClient(TxnId txn_id, api::Response&& res);

  TxnId NextTxnId();

  RateLimiter rate_limiter_;
  TxnId txn_id_counter_;

  std::string debug_tag_;

  struct PendingResponse {
    std::string identity;
    uint32_t stream_id;

    explicit PendingResponse(std::string identity, uint32_t stream_id)
        : identity(identity), stream_id(stream_id) {}
  };

  std::unordered_map<TxnId, PendingResponse> pending_responses_;
  std::unordered_map<TxnId, std::unique_ptr<FinishedTransaction>> finished_txns_;

  std::unordered_set<MachineId> offline_machines_;
};

}  // namespace slog