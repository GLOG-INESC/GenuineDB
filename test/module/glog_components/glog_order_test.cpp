#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "common/configuration.h"
#include "common/constants.h"

#include "test/glog_test_utils.h"

using namespace std;
using namespace slog;
using namespace glog;

using std::chrono::milliseconds;
using slog::internal::Envelope;
using slog::internal::AddClientRequest;

int num_regions = 1;
class GlogOrderTestRegister : public ::testing::Test {
 protected:
  static const size_t NUM_MACHINES = 2;

  void SetUp() {
    internal::Configuration extra_config;
    extra_config.set_epoch_duration(milliseconds(1000).count());
    extra_config.set_num_stubs(1);

    configs = MakeGlogTestConfigurations("glog_order", num_regions /* num_regions */, 1 /* num_replicas */,
                                         1 /* num_partitions */, extra_config);
    for (size_t i = 0; i < NUM_MACHINES; i++) {
      test_geolog[i] = make_unique<TestGlog>(configs[i]);
      if (i == 1){
        test_geolog[i]->AddOrder();
        continue;
      }
      test_geolog[i]->AddGlogOutputSocket(kGLOGChannel);
      test_geolog[i]->AddOutputSocket(kMultiHomeOrdererChannel);
      test_geolog[i]->AddOutputSocket(kMultiHomeOrdererChannel+1);

      senders_[i] = test_geolog[i]->NewSender();
    }

    for (const auto& test_slog : test_geolog) {
      test_slog->StartGlogInNewThreads();
    }
  }

  internal::ZiplogClientNextEpochRequest* ReceiveNewClientEpoch(int i, int stub_id){
    auto req_env = test_geolog[i]->ReceiveFromOutputSocket(kMultiHomeOrdererChannel+stub_id);
    if (req_env == nullptr) {
      return {};
    }

    if (req_env->response().type_case() != internal::Response::kClientEpoch) {
      return {};
    }

    return req_env->mutable_response()->release_client_epoch();
  }

  void SendToOrder(int i, EnvelopePtr&& req){ senders_[i]->Send(std::move(req), configs[i]->order_machine_id(), kZipOrderChannel); }

  internal::ZiplogNextEpochRequest* ReceiveNewEpoch(int i) {
    auto req_env = test_geolog[i]->ReceiveFromOutputSocket(kGLOGChannel, false);
    if (req_env == nullptr) {
      return {};
    }

    if (req_env->request().type_case() != internal::Request::kOrderEpoch) {
      return {};
    }

    return req_env->mutable_request()->release_order_epoch();

  }

  internal::ZiplogFinishClient* ReceiveClientRemoval(int i, int log_id) {
    auto req_env = test_geolog[i]->ReceiveFromOutputSocketTimeout(kLogManagerChannel + log_id);
    if (req_env == nullptr) {
      return {};
    }

    if (req_env->request().type_case() != internal::Request::kZipFinishClient) {
      return {};
    }

    return req_env->mutable_request()->release_zip_finish_client();

  }

  void MakeAddClientRequest(AddClientRequest* client_registration, uint64_t client_id, uint64_t num_slots, uint32_t region, uint32_t machine_id){
    client_registration->set_client_id(client_id);
    client_registration->set_num_slots(num_slots);
    client_registration->set_region(region);
    client_registration->set_machine_id(machine_id);
  }

  unique_ptr<Sender> senders_[NUM_MACHINES];
  unique_ptr<TestGlog> test_geolog[NUM_MACHINES];
  ConfigVec configs;
};

TEST_F(GlogOrderTestRegister, RegisterStub) {
  uint64_t client_id = 0;
  uint64_t slots = 50;
  RegionId client_region = 1;

  auto env = make_unique<Envelope>();
  auto add_client = env->mutable_request()->mutable_add_client();

  MakeAddClientRequest(add_client, client_id, slots, client_region, MakeMachineId(client_region, 0, 0));
  SendToOrder(0, std::move(env));

  sleep(2);
  auto new_epoch = ReceiveNewEpoch(0);
  ASSERT_TRUE(new_epoch != nullptr);
  ASSERT_EQ(1, new_epoch->regions_size());
  auto epoch_info = new_epoch->regions(0);
  ASSERT_EQ(0, epoch_info.gsn_base());

  ASSERT_EQ(1, epoch_info.num_clients());
  ASSERT_EQ(1, epoch_info.slots_size());
  ASSERT_EQ(client_id, epoch_info.slots(0).client_id());
  ASSERT_EQ(slots, epoch_info.slots(0).num_slots());
  ASSERT_EQ(client_region, epoch_info.slots(0).client_region());
}

TEST_F(GlogOrderTestRegister, RegisterStubNextEpoch) {
  uint64_t client_id = 0;
  uint64_t slots = 50;
  auto current_time = std::chrono::high_resolution_clock::now();

  auto env = make_unique<Envelope>();
  auto add_client = env->mutable_request()->mutable_add_client();

  MakeAddClientRequest(add_client, client_id, slots, 0, MakeMachineId(0, 0, 0));
  SendToOrder(0, std::move(env));

  auto next_epoch = ReceiveNewClientEpoch(0, 0);

  ASSERT_EQ(slots, next_epoch->num_slots());
}

TEST_F(GlogOrderTestRegister, StubReceivesNewEpochs) {
  uint64_t client_id = 0;
  uint64_t slots = 50;
  auto current_time = std::chrono::high_resolution_clock::now();

  auto env = make_unique<Envelope>();
  auto add_client = env->mutable_request()->mutable_add_client();

  MakeAddClientRequest(add_client, client_id, slots, 0, MakeMachineId(0, 0, 0));
  SendToOrder(0, std::move(env));

  auto next_epoch = ReceiveNewClientEpoch(0, 0);

  ASSERT_EQ(slots, next_epoch->num_slots());

  //auto next_epoch_2 = ReceiveNewClientEpoch(0, 0);

  //ASSERT_EQ(slots, next_epoch_2->num_slots());
}

TEST_F(GlogOrderTestRegister, RegisterMultipleStubsNextEpoch) {
  uint64_t client_id = 0;
  uint64_t slots = 50;
  uint64_t client_id_2 = 1;
  uint64_t slots_2 = 100;

  auto current_time = std::chrono::high_resolution_clock::now();

  {
  auto env = make_unique<Envelope>();
  auto add_client = env->mutable_request()->mutable_add_client();

  MakeAddClientRequest(add_client, client_id, slots, 0, MakeMachineId(0, 0, 0));
  SendToOrder(0, std::move(env));
  }

  {
    auto env = make_unique<Envelope>();
    auto add_client = env->mutable_request()->mutable_add_client();

    MakeAddClientRequest(add_client, client_id_2, slots_2, 0, MakeMachineId(0, 0, 0));
    SendToOrder(0, std::move(env));
  }

  auto next_epoch = ReceiveNewClientEpoch(0, 0);
  auto next_epoch_2 = ReceiveNewClientEpoch(0, 1);

  ASSERT_NE(next_epoch->num_slots(), slots);
  ASSERT_NE(next_epoch_2->num_slots(), slots_2);
}

TEST_F(GlogOrderTestRegister, AdvanceGsnBase) {
  uint64_t client_id = 0;
  uint64_t slots = 50;
  RegionId client_region = 1;

  {
    auto env = make_unique<Envelope>();
    auto add_client = env->mutable_request()->mutable_add_client();

    MakeAddClientRequest(add_client, client_id, slots, client_region, MakeMachineId(client_region, 0, 0));
    SendToOrder(0, std::move(env));
  }

  sleep(2);
  auto new_epoch = ReceiveNewEpoch(0);
  ASSERT_TRUE(new_epoch != nullptr);

  new_epoch = ReceiveNewEpoch(0);

  ASSERT_TRUE(new_epoch != nullptr);
  ASSERT_EQ(1, new_epoch->regions_size());

  auto epoch_info = new_epoch->regions(0);
  ASSERT_EQ(50, epoch_info.gsn_base());
  ASSERT_EQ(1, epoch_info.num_clients());
  ASSERT_EQ(1, epoch_info.slots_size());
  ASSERT_EQ(client_id, epoch_info.slots(0).client_id());
  ASSERT_EQ(slots, epoch_info.slots(0).num_slots());
}

TEST_F(GlogOrderTestRegister, ChangeClientRate) {
  uint64_t client_id = 0;
  uint64_t slots_before = 50;
  uint64_t slots_after = 100;
  RegionId client_region = 1;

  {
    auto env = make_unique<Envelope>();
    auto add_client = env->mutable_request()->mutable_add_client();

    MakeAddClientRequest(add_client, client_id, slots_before, client_region, MakeMachineId(client_region, 0, 0));
    SendToOrder(0, std::move(env));
  }

  {
    auto new_epoch = ReceiveNewEpoch(0);
    while (new_epoch == nullptr){
      new_epoch = ReceiveNewEpoch(0);
    }
    ASSERT_TRUE(new_epoch != nullptr);
    auto epoch_info = new_epoch->regions(0);

    ASSERT_EQ(0, epoch_info.region());
    ASSERT_EQ(0, epoch_info.gsn_base());
    ASSERT_EQ(1, epoch_info.num_clients());
    ASSERT_EQ(1, epoch_info.slots_size());
    ASSERT_EQ(client_id, epoch_info.slots(0).client_id());
    ASSERT_EQ(slots_before, epoch_info.slots(0).num_slots());
  }

  auto env = make_unique<Envelope>();
  auto update_rate = env->mutable_request()->mutable_update_rate();
  update_rate->set_client_id(client_id);
  update_rate->set_num_slots(slots_after);
  SendToOrder(0, std::move(env));

  {
    auto new_epoch = ReceiveNewEpoch(0);
    while (new_epoch == nullptr){
      new_epoch = ReceiveNewEpoch(0);
    }
    ASSERT_TRUE(new_epoch != nullptr);

    auto epoch_info = new_epoch->regions(0);
    ASSERT_EQ(0, epoch_info.region());
    ASSERT_EQ(slots_before, epoch_info.gsn_base());
    ASSERT_EQ(1, epoch_info.num_clients());
    ASSERT_EQ(1, epoch_info.slots_size());
    ASSERT_EQ(client_id, epoch_info.slots(0).client_id());
    ASSERT_EQ(slots_after, epoch_info.slots(0).num_slots());
  }
}



int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  google::InstallFailureSignalHandler();
  return RUN_ALL_TESTS();
}

