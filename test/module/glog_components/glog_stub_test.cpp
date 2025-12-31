#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "common/configuration.h"
#include "common/constants.h"
#include "test/glog_test_utils.h"

using namespace std;
using namespace slog;
using namespace glog;
using internal::Envelope;
using std::chrono::milliseconds;

class GLOGStubTest : public ::testing::Test {
 protected:
  static const size_t NUM_REGIONS = 2;
  static const size_t NUM_REPLICAS = 2;
  static const size_t NUM_PARTITIONS = 2;


  void SetUp() {
    internal::Configuration extra_config;
    extra_config.set_epoch_duration(milliseconds(10).count());
    extra_config.set_smoothing(2);
    extra_config.set_measurements(1);
    extra_config.set_increment(0.1);
    extra_config.set_noops(0.25);
    extra_config.set_start_slots(1);
    extra_config.set_min_slots(1);
    extra_config.set_rounding_factor(1);
    extra_config.mutable_simple_partitioning()->set_num_records(2500000);
    extra_config.mutable_simple_partitioning()->set_record_size_bytes(100);

    int num_stubs = 1;


    configs_ = MakeGlogTestConfigurations("client_stub", NUM_REGIONS, NUM_REPLICAS, NUM_PARTITIONS, extra_config);


    int machine = 0;
    for (size_t r = 0; r < NUM_REGIONS; r++) {
      for (size_t rep = 0; rep < NUM_REPLICAS; rep++) {
        for (size_t p = 0; p < NUM_PARTITIONS; p++) {
          auto machine_id = MakeMachineId(r, rep, p);
          test_geolog[machine_id] = make_unique<TestGlog>(configs_[machine]);

          // Only add a client stub to the first machine
          if (machine == 0){
            test_geolog[machine_id]->AddGlogStubs(num_stubs);
          }
          test_geolog[machine_id]->AddGlogOutputSocket(kGLOGChannel);

          senders_.emplace(MakeMachineId(r, rep, p), test_geolog[machine_id]->NewSender());
          machine++;

          if (p == 0){
            test_geolog[machine_id]->Data("0", {"xxxxx", 0, 0});
            test_geolog[machine_id]->Data("2", {"xxxxx", 1, 0});
          } else {
            test_geolog[machine_id]->Data("1", {"xxxxx", 0, 0});
            test_geolog[machine_id]->Data("3", {"xxxxx", 1, 0});
          }
        }
      }
    }

    test_geolog[numeric_limits<MachineId>::max()] = make_unique<TestGlog>(configs_[machine]);
    test_geolog[numeric_limits<MachineId>::max()]->AddGlogOutputSocket(kZipOrderChannel);

    for (size_t r = 0; r < NUM_REGIONS; r++) {
      for (size_t rep = 0; rep < NUM_REPLICAS; rep++) {
        for (size_t p = 0; p < NUM_PARTITIONS; p++) {
          auto machine_id = MakeMachineId(r, rep, p);

          test_geolog[machine_id]->StartGlogInNewThreads();

        }
      }
    }
    test_geolog[numeric_limits<MachineId>::max()]->StartGlogInNewThreads();

    client_id_ = MakeClientId(configs_[0]->local_region(), configs_[0]->local_replica(), configs_[0]->local_partition(), 0);
  }

  void SendToGLOGStub(MachineId from, MachineId to, EnvelopePtr&& req, int client_stub) {
    auto it = senders_.find(from);
    CHECK(it != senders_.end());
    it->second->Send(std::move(req), to, kGLOGStubChannel + client_stub);
  }

  slog::internal::AddClientRequest* ReceiveAddClientRequest(){
    EnvelopePtr req_env = test_geolog[numeric_limits<MachineId>::max()]->ReceiveFromOutputSocket(kZipOrderChannel, false);

    if (req_env == nullptr) {
      return nullptr;
    }

    if (req_env->request().type_case() != slog::internal::Request::kAddClient) {
      return nullptr;
    }

    return req_env->mutable_request()->release_add_client();
  }

  slog::internal::UpdateRateRequest* ReceiveRateUpdateRequest(){
    EnvelopePtr req_env = test_geolog[numeric_limits<MachineId>::max()]->ReceiveFromOutputSocket(kZipOrderChannel, false);

    if (req_env == nullptr) {
      return nullptr;
    }

    if (req_env->request().type_case() != slog::internal::Request::kUpdateRate) {
      return nullptr;
    }

    return req_env->mutable_request()->release_update_rate();
  }

  slog::internal::RemoveClientRequest* ReceiveClientFinishRequest(){
    EnvelopePtr req_env = test_geolog[numeric_limits<MachineId>::max()]->ReceiveFromOutputSocket(kZipOrderChannel, false);

    if (req_env == nullptr) {
      return nullptr;
    }

    if (req_env->request().type_case() != slog::internal::Request::kRemoveClient) {
      return nullptr;
    }

    return req_env->mutable_request()->release_remove_client();
  }

  slog::internal::ForwardZiplogBatch* ReceiveBatch(MachineId i, bool timeout = false) {

    EnvelopePtr req_env;

    if (timeout){
      req_env = test_geolog[i]->ReceiveFromOutputSocketTimeout(kGLOGChannel, false, milliseconds(1000));
    } else {
      req_env = test_geolog[i]->ReceiveFromOutputSocket(kGLOGChannel, false);
    }

    if (req_env == nullptr) {
      return nullptr;
    }

    if (req_env->request().type_case() != slog::internal::Request::kZipForwardBatch) {
      return nullptr;
    }

    return req_env->mutable_request()->release_zip_forward_batch();
  }


  bool ReceiveStubTermination(MachineId id) {
    auto req_env = test_geolog[id]->ReceiveFromOutputSocket(kZipTxnGenerator);
    if (req_env == nullptr) {
      return false;
    }
    if (req_env->response().type_case() != internal::Response::kClientFinishResponse) {
      return false;
    }
    return req_env->mutable_response()->mutable_client_finish_response()->success();
  }


  unordered_map<MachineId, unique_ptr<TestGlog>> test_geolog;
  ConfigVec configs_;
  unordered_map<MachineId, unique_ptr<Sender>> senders_;
  ClientId client_id_;
};

TEST_F(GLOGStubTest, RegisterClientStubOrder){
    string address;

    auto client_registration = ReceiveAddClientRequest();
    auto region = test_geolog[0]->config()->local_region();
    auto replica = test_geolog[0]->config()->local_replica();
    auto partition = test_geolog[0]->config()->local_partition();

    ASSERT_NE(client_registration, nullptr);
    ASSERT_EQ(test_geolog[0]->config()->start_slots(), client_registration->num_slots());
    ASSERT_EQ(MakeClientId(region, replica, partition, 0), client_registration->client_id());
    ASSERT_EQ(region, client_registration->region());
    ASSERT_EQ(slog::MakeMachineId(region, replica, partition), client_registration->machine_id());
}

// Verify that the client sends the request
TEST_F(GLOGStubTest, SendRequest1Region) {

  auto send_txn_1 = MakeTestTransaction(configs_[0], 1000,
                                            {{"0", KeyType::READ, 0}});
  send_txn_1->mutable_internal()->clear_involved_regions();
  send_txn_1->mutable_internal()->add_involved_regions(0);

  auto expected_txn_1 = *send_txn_1;

  {
    EnvelopePtr req = std::make_unique<Envelope>();

    auto forward_request = req->mutable_request()->mutable_forward_txn();
    forward_request->set_allocated_txn(send_txn_1);
    SendToGLOGStub(MakeMachineId(0, 0, 0), MakeMachineId(0, 0, 0), std::move(req), 0);
  }

  string address;
  auto client_registration = ReceiveAddClientRequest();
  ASSERT_NE(client_registration, nullptr);

  EnvelopePtr epoch = std::make_unique<Envelope>();
  auto client_epoch = epoch->mutable_response()->mutable_client_epoch();
  client_epoch->set_num_slots(1);
  client_epoch->set_begin(std::chrono::high_resolution_clock::now().time_since_epoch().count());

  SendToGLOGStub(MakeMachineId(0, 0, 0), MakeMachineId(0, 0, 0), std::move(epoch), 0);

  //Txn should be sent to regions 0, but not to region 1

  // Check the stub issues operation to all local replicas
  for (size_t rep = 0; rep < NUM_REPLICAS; rep++) {
    MachineId machine_id = MakeMachineId(0, rep, 0);
    auto forward_batch = ReceiveBatch(machine_id);
    ASSERT_NE(forward_batch, nullptr);
    ASSERT_EQ(forward_batch->generator(), client_id_);
    ASSERT_EQ(forward_batch->generator_position(), 0);
    ASSERT_EQ(forward_batch->slots(), 1);
    ASSERT_EQ(forward_batch->home(), 0);
    ASSERT_EQ(forward_batch->batch_data_size(), 1);
    auto batch = forward_batch->mutable_batch_data(0);
    ASSERT_EQ(batch->transactions_size(), 1);
    auto txn = batch->mutable_transactions(0);
    ASSERT_EQ(1000, txn->internal().id());
    ASSERT_EQ(1, txn->keys_size());
    ASSERT_EQ(TxnValueEntry(expected_txn_1, "0"), TxnValueEntry(*txn, "0"));
  }

  // And that it issues them to remote regions
  for (size_t r = 1; r < NUM_REGIONS; r++) {
    MachineId machine_id = MakeMachineId(r, 0, 0);
    auto forward_batch = ReceiveBatch(machine_id);
    ASSERT_EQ(forward_batch->generator(), client_id_);
    ASSERT_EQ(forward_batch->generator_position(), 0);
    ASSERT_EQ(forward_batch->slots(), 1);
    ASSERT_EQ(forward_batch->home(), 0);
    ASSERT_EQ(forward_batch->batch_data_size(), 1);
    auto batch = forward_batch->mutable_batch_data(0);
    ASSERT_EQ(batch->transactions_size(), 1);
    auto txn = batch->mutable_transactions(0);
    ASSERT_EQ(txn->mutable_internal()->noop(), true);
  }

}

TEST_F(GLOGStubTest, SendRequest2Region) {

  auto send_txn_1 = MakeTestTransaction(configs_[0], 1000,
                                        {{"0", KeyType::READ, 0}});
  send_txn_1->mutable_internal()->clear_involved_regions();
  send_txn_1->mutable_internal()->add_involved_regions(0);
  send_txn_1->mutable_internal()->add_involved_regions(1);

  auto expected_txn_1 = *send_txn_1;

  {
    EnvelopePtr req = std::make_unique<Envelope>();

    auto forward_request = req->mutable_request()->mutable_forward_txn();
    forward_request->set_allocated_txn(send_txn_1);
    SendToGLOGStub(MakeMachineId(0, 0, 0), MakeMachineId(0, 0, 0), std::move(req), 0);
  }

  string address;
  auto client_registration = ReceiveAddClientRequest();
  ASSERT_NE(client_registration, nullptr);

  EnvelopePtr epoch = std::make_unique<Envelope>();
  auto client_epoch = epoch->mutable_response()->mutable_client_epoch();
  client_epoch->set_num_slots(1);
  client_epoch->set_begin(std::chrono::high_resolution_clock::now().time_since_epoch().count());

  SendToGLOGStub(MakeMachineId(0, 0, 0), MakeMachineId(0, 0, 0), std::move(epoch), 0);

  //Txn should be sent to regions 0, but not to region 1

  // Check the stub issues operation to all local replicas
  for (size_t rep = 0; rep < NUM_REPLICAS; rep++) {
    MachineId machine_id = MakeMachineId(0, rep, 0);
    auto forward_batch = ReceiveBatch(machine_id);
    ASSERT_NE(forward_batch, nullptr);
    ASSERT_EQ(forward_batch->generator(), client_id_);
    ASSERT_EQ(forward_batch->generator_position(), 0);
    ASSERT_EQ(forward_batch->slots(), 1);
    ASSERT_EQ(forward_batch->home(), 0);
    ASSERT_EQ(forward_batch->batch_data_size(), 1);
    auto batch = forward_batch->mutable_batch_data(0);
    ASSERT_EQ(batch->transactions_size(), 1);
    auto txn = batch->mutable_transactions(0);
    ASSERT_EQ(1000, txn->internal().id());
    ASSERT_EQ(1, txn->keys_size());
    ASSERT_EQ(TxnValueEntry(expected_txn_1, "0"), TxnValueEntry(*txn, "0"));
  }

  // And that it issues them to remote regions
  for (size_t r = 1; r < NUM_REGIONS; r++) {
    MachineId machine_id = MakeMachineId(r, 0, 0);
    auto forward_batch = ReceiveBatch(machine_id);
    ASSERT_EQ(forward_batch->generator(), client_id_);
    ASSERT_EQ(forward_batch->generator_position(), 0);
    ASSERT_EQ(forward_batch->slots(), 1);
    ASSERT_EQ(forward_batch->home(), 0);
    ASSERT_EQ(forward_batch->batch_data_size(), 1);
    auto batch = forward_batch->mutable_batch_data(0);
    ASSERT_EQ(batch->transactions_size(), 1);
    auto txn = batch->mutable_transactions(0);
    ASSERT_EQ(1000, txn->internal().id());
    ASSERT_EQ(1, txn->keys_size());
    ASSERT_EQ(TxnValueEntry(expected_txn_1, "0"), TxnValueEntry(*txn, "0"));
  }

}



// Verify that the client sends a noop
TEST_F(GLOGStubTest, SendNoop) {


  auto client_registration = ReceiveAddClientRequest();
  ASSERT_NE(client_registration, nullptr);

  EnvelopePtr epoch = std::make_unique<Envelope>();
  auto client_epoch = epoch->mutable_response()->mutable_client_epoch();
  client_epoch->set_num_slots(2);
  client_epoch->set_begin(std::chrono::high_resolution_clock::now().time_since_epoch().count());

  SendToGLOGStub(MakeMachineId(0, 0, 0), MakeMachineId(0, 0, 0), std::move(epoch), 0);

  for (int noop = 0; noop < 2; noop++){
    // Check the stub issues operation to all local replicas
    for (size_t rep = 0; rep < NUM_REPLICAS; rep++) {
      MachineId machine_id = MakeMachineId(0, rep, 0);
      auto forward_batch = ReceiveBatch(machine_id);
      ASSERT_EQ(forward_batch->generator(), client_id_);
      ASSERT_EQ(forward_batch->generator_position(), noop);
      ASSERT_EQ(forward_batch->slots(), 1);
      ASSERT_EQ(forward_batch->home(), 0);
      ASSERT_EQ(forward_batch->batch_data_size(), 1);
      auto batch = forward_batch->mutable_batch_data(0);
      ASSERT_EQ(batch->transactions_size(), 1);
      auto txn = batch->mutable_transactions(0);
      ASSERT_TRUE(txn->internal().noop());
    }

    // And that it issues them to remote regions
    for (size_t r = 1; r < NUM_REGIONS; r++) {
      MachineId machine_id = MakeMachineId(r, 0, 0);
      auto forward_batch = ReceiveBatch(machine_id);
      ASSERT_EQ(forward_batch->generator(), client_id_);
      ASSERT_EQ(forward_batch->generator_position(), noop);
      ASSERT_EQ(forward_batch->slots(), 1);
      ASSERT_EQ(forward_batch->home(), 0);
      ASSERT_EQ(forward_batch->batch_data_size(), 1);
      auto batch = forward_batch->mutable_batch_data(0);
      ASSERT_EQ(batch->transactions_size(), 1);
      auto txn = batch->mutable_transactions(0);
      ASSERT_TRUE(txn->internal().noop());
    }
  }

}

// Verify that EMA updates correctly
TEST_F(GLOGStubTest, UpdateEMA) {
  // Receive registration message

  auto client_registration = ReceiveAddClientRequest();
  ASSERT_NE(client_registration, nullptr);
  {

  EnvelopePtr epoch = std::make_unique<Envelope>();
  auto client_epoch = epoch->mutable_response()->mutable_client_epoch();
  client_epoch->set_num_slots(1);
  client_epoch->set_begin(std::chrono::high_resolution_clock::now().time_since_epoch().count());
  SendToGLOGStub(MakeMachineId(0, 0, 0), MakeMachineId(0, 0, 0), std::move(epoch), 0);
  }

  auto expected_txn_1 = MakeTestTransaction(configs_[0], 1000,
                                            {{"A", KeyType::READ, 0}, {"B", KeyType::READ, 0}, {"C", KeyType::WRITE, 0}});
  auto expected_txn_2 = MakeTestTransaction(configs_[0], 1001,
                                            {{"A", KeyType::READ, 0}, {"B", KeyType::READ, 0}, {"C", KeyType::WRITE, 0}});

  {
    EnvelopePtr req = std::make_unique<Envelope>();

    auto forward_request = req->mutable_request()->mutable_forward_txn();
    forward_request->set_allocated_txn(expected_txn_1);
    SendToGLOGStub(MakeMachineId(0, 0, 0), MakeMachineId(0, 0, 0), std::move(req), 0);
  }

  {
    EnvelopePtr req = std::make_unique<Envelope>();

    auto forward_request = req->mutable_request()->mutable_forward_txn();
    forward_request->set_allocated_txn(expected_txn_2);
    SendToGLOGStub(MakeMachineId(0, 0, 0), MakeMachineId(0, 0, 0), std::move(req), 0);
  }

  // Give time for the requests to arrive first
  sleep(2);
  {

  EnvelopePtr epoch = std::make_unique<Envelope>();
  auto client_epoch = epoch->mutable_response()->mutable_client_epoch();
  client_epoch->set_num_slots(1);
  client_epoch->set_begin(std::chrono::high_resolution_clock::now().time_since_epoch().count());

  SendToGLOGStub(MakeMachineId(0, 0, 0), MakeMachineId(0, 0, 0), std::move(epoch), 0);
  }


  auto update_request = ReceiveRateUpdateRequest();

  ASSERT_EQ(3, update_request->num_slots());
  ASSERT_EQ(client_id_, update_request->client_id());

}

// Verify it sends the correct amount of requests
TEST_F(GLOGStubTest, CorrectBatching) {
  auto send_txn_1 = MakeTestTransaction(configs_[0], 1000,
                                        {{"0", KeyType::READ, 0}});
  send_txn_1->mutable_internal()->clear_involved_regions();
  send_txn_1->mutable_internal()->add_involved_regions(0);

  auto send_txn_2 = MakeTestTransaction(configs_[0], 1001,
                                        {{"1", KeyType::READ, 0}});
  send_txn_2->mutable_internal()->clear_involved_regions();
  send_txn_2->mutable_internal()->add_involved_regions(0);

  auto expected_txn_1 = *send_txn_1;
  auto expected_txn_2 = *send_txn_2;

  {
    EnvelopePtr req = std::make_unique<Envelope>();

    auto forward_request = req->mutable_request()->mutable_forward_txn();
    forward_request->set_allocated_txn(send_txn_1);
    SendToGLOGStub(MakeMachineId(0, 0, 0), MakeMachineId(0, 0, 0), std::move(req), 0);
  }

  {
    EnvelopePtr req = std::make_unique<Envelope>();

    auto forward_request = req->mutable_request()->mutable_forward_txn();
    forward_request->set_allocated_txn(send_txn_2);
    SendToGLOGStub(MakeMachineId(0, 0, 0), MakeMachineId(0, 0, 0), std::move(req), 0);
  }


  sleep(2);

  auto client_registration = ReceiveAddClientRequest();
  ASSERT_NE(client_registration, nullptr);

  EnvelopePtr epoch = std::make_unique<Envelope>();
  auto client_epoch = epoch->mutable_response()->mutable_client_epoch();
  client_epoch->set_num_slots(1);
  client_epoch->set_begin(std::chrono::high_resolution_clock::now().time_since_epoch().count());
  SendToGLOGStub(MakeMachineId(0, 0, 0), MakeMachineId(0, 0, 0), std::move(epoch), 0);


  // Check the stub issues operation to all local replicas
  for (size_t rep = 0; rep < NUM_REPLICAS; rep++) {
    MachineId machine_id = MakeMachineId(0, rep, 0);
    auto forward_batch = ReceiveBatch(machine_id);
    ASSERT_EQ(forward_batch->generator(), client_id_);
    ASSERT_EQ(forward_batch->generator_position(), 0);
    ASSERT_EQ(forward_batch->slots(), 1);
    ASSERT_EQ(forward_batch->home(), 0);
    ASSERT_EQ(forward_batch->batch_data_size(), 1);
    auto batch = forward_batch->mutable_batch_data(0);
    ASSERT_EQ(batch->transactions_size(), 2);

    auto txn = batch->mutable_transactions(0);
    ASSERT_EQ(1000, txn->internal().id());
    ASSERT_EQ(1, txn->keys_size());
    ASSERT_EQ(TxnValueEntry(expected_txn_1, "0"), TxnValueEntry(*txn, "0"));

    txn = batch->mutable_transactions(1);
    ASSERT_EQ(1001, txn->internal().id());
    ASSERT_EQ(1, txn->keys_size());
    ASSERT_EQ(TxnValueEntry(expected_txn_2, "1"), TxnValueEntry(*txn, "1"));
  }

  // And that it issues them to remote regions
  for (size_t r = 1; r < NUM_REGIONS; r++) {
    MachineId machine_id = MakeMachineId(r, 0, 0);
    auto forward_batch = ReceiveBatch(machine_id);
    ASSERT_EQ(forward_batch->generator(), client_id_);
    ASSERT_EQ(forward_batch->generator_position(), 0);
    ASSERT_EQ(forward_batch->slots(), 1);
    ASSERT_EQ(forward_batch->home(), 0);
    ASSERT_EQ(forward_batch->batch_data_size(), 1);
    auto batch = forward_batch->mutable_batch_data(0);
    ASSERT_EQ(batch->transactions_size(), 1);

    auto txn = batch->mutable_transactions(0);
    ASSERT_EQ(txn->internal().noop(), true);
  }

}

// Verify it stub sends after getting more slots
TEST_F(GLOGStubTest, MoreMessagesNextEpoch) {
  auto send_txn_1 = MakeTestTransaction(configs_[0], 1000,
                                        {{"0", KeyType::READ, 0}});
  send_txn_1->mutable_internal()->clear_involved_regions();
  send_txn_1->mutable_internal()->add_involved_regions(0);

  auto send_txn_2 = MakeTestTransaction(configs_[0], 1001,
                                        {{"1", KeyType::READ, 0}});
  send_txn_2->mutable_internal()->clear_involved_regions();
  send_txn_2->mutable_internal()->add_involved_regions(0);

  auto expected_txn_1 = *send_txn_1;
  auto expected_txn_2 = *send_txn_2;

  {
    EnvelopePtr req = std::make_unique<Envelope>();

    auto forward_request = req->mutable_request()->mutable_forward_txn();
    forward_request->set_allocated_txn(send_txn_1);
    SendToGLOGStub(MakeMachineId(0, 0, 0), MakeMachineId(0, 0, 0), std::move(req), 0);
  }

  sleep(2);
  auto client_registration = ReceiveAddClientRequest();
  ASSERT_NE(client_registration, nullptr);
  {

  EnvelopePtr epoch = std::make_unique<Envelope>();
  auto client_epoch = epoch->mutable_response()->mutable_client_epoch();
  client_epoch->set_num_slots(1);
  client_epoch->set_begin(std::chrono::high_resolution_clock::now().time_since_epoch().count());
  SendToGLOGStub(MakeMachineId(0, 0, 0), MakeMachineId(0, 0, 0), std::move(epoch), 0);
  }



  // Check the stub issues operation to all local replicas
  for (size_t rep = 0; rep < NUM_REPLICAS; rep++) {
    MachineId machine_id = MakeMachineId(0, rep, 0);
    auto forward_batch = ReceiveBatch(machine_id);
    ASSERT_NE(forward_batch, nullptr);

    ASSERT_EQ(forward_batch->generator(), client_id_);
    ASSERT_EQ(forward_batch->generator_position(), 0);
    ASSERT_EQ(forward_batch->slots(), 1);
    ASSERT_EQ(forward_batch->home(), 0);
    ASSERT_EQ(forward_batch->batch_data_size(), 1);
    auto batch = forward_batch->mutable_batch_data(0);
    ASSERT_EQ(batch->transactions_size(), 1);

    auto txn = batch->mutable_transactions(0);
    ASSERT_EQ(1000, txn->internal().id());
    ASSERT_EQ(1, txn->keys_size());
    ASSERT_EQ(TxnValueEntry(expected_txn_1, "0"), TxnValueEntry(*txn, "0"));
  }

  // And that it issues them to remote regions
  for (size_t r = 1; r < NUM_REGIONS; r++) {
    MachineId machine_id = MakeMachineId(r, 0, 0);
    auto forward_batch = ReceiveBatch(machine_id);
    ASSERT_NE(forward_batch, nullptr);

    ASSERT_EQ(forward_batch->generator(), client_id_);
    ASSERT_EQ(forward_batch->generator_position(), 0);
    ASSERT_EQ(forward_batch->slots(), 1);
    ASSERT_EQ(forward_batch->home(), 0);
    ASSERT_EQ(forward_batch->batch_data_size(), 1);
    auto batch = forward_batch->mutable_batch_data(0);
    ASSERT_EQ(batch->transactions_size(), 1);

    auto txn = batch->mutable_transactions(0);
    ASSERT_EQ(txn->internal().noop(), true);
  }

  // Send second message
  {
    EnvelopePtr req = std::make_unique<Envelope>();

    auto forward_request = req->mutable_request()->mutable_forward_txn();
    forward_request->set_allocated_txn(send_txn_2);
    SendToGLOGStub(MakeMachineId(0, 0, 0), MakeMachineId(0, 0, 0), std::move(req), 0);
  }
  sleep(2);
  {

    auto forward_batch = ReceiveBatch(MakeMachineId(0, 0, 0), true);
    ASSERT_EQ(forward_batch, nullptr);
  }

  {
    EnvelopePtr epoch = std::make_unique<Envelope>();
    auto client_epoch = epoch->mutable_response()->mutable_client_epoch();
    client_epoch->set_num_slots(1);
    client_epoch->set_begin(std::chrono::high_resolution_clock::now().time_since_epoch().count());
    SendToGLOGStub(MakeMachineId(0, 0, 0), MakeMachineId(0, 0, 0), std::move(epoch), 0);

  }
  sleep(2);

  // Check the stub issues operation to all local replicas
  for (size_t rep = 0; rep < NUM_REPLICAS; rep++) {
    MachineId machine_id = MakeMachineId(0, rep, 0);
    auto forward_batch = ReceiveBatch(machine_id);
    ASSERT_NE(forward_batch, nullptr);
    ASSERT_EQ(forward_batch->generator(), client_id_);
    ASSERT_EQ(forward_batch->generator_position(), 1);
    ASSERT_EQ(forward_batch->slots(), 1);
    ASSERT_EQ(forward_batch->home(), 0);
    ASSERT_EQ(forward_batch->batch_data_size(), 1);
    auto batch = forward_batch->mutable_batch_data(0);
    ASSERT_EQ(batch->transactions_size(), 1);

    auto txn = batch->mutable_transactions(0);
    ASSERT_EQ(1001, txn->internal().id());
    ASSERT_EQ(1, txn->keys_size());
    ASSERT_EQ(TxnValueEntry(expected_txn_2, "1"), TxnValueEntry(*txn, "1"));
  }

  // And that it issues them to remote regions
  for (size_t r = 1; r < NUM_REGIONS; r++) {
    MachineId machine_id = MakeMachineId(r, 0, 0);
    auto forward_batch = ReceiveBatch(machine_id);
    ASSERT_EQ(forward_batch->generator(), client_id_);
    ASSERT_EQ(forward_batch->generator_position(), 1);
    ASSERT_EQ(forward_batch->slots(), 1);
    ASSERT_EQ(forward_batch->home(), 0);
    ASSERT_EQ(forward_batch->batch_data_size(), 1);
    auto batch = forward_batch->mutable_batch_data(0);
    ASSERT_EQ(batch->transactions_size(), 1);

    auto txn = batch->mutable_transactions(0);
    ASSERT_EQ(txn->internal().noop(), true);
  }

}

TEST_F(GLOGStubTest, SendClientFinish) {
  {
    EnvelopePtr req = std::make_unique<Envelope>();

    auto client_finish = req->mutable_request()->mutable_zip_finish_client();
    client_finish->set_client_id(1);

    SendToGLOGStub(MakeMachineId(0, 0, 0), MakeMachineId(0, 0, 0), std::move(req), 0);
  }

  sleep(2);
  // Ensure Order module receives it
  auto client_finish_request = ReceiveClientFinishRequest();
  ASSERT_EQ(client_id_, client_finish_request->client_id());
}

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_v = 3;
  google::InstallFailureSignalHandler();
  return RUN_ALL_TESTS();
}