#include <memory>
#include <vector>

#include "common/configuration.h"
#include "common/metrics.h"
#include "connection/broker.h"
#include "module/clock_synchronizer.h"
#include "module/glog/glog_order.h"

#include "service/service_utils.h"
#include "version.h"

DEFINE_string(config, "slog.conf", "Path to the configuration file");
DEFINE_string(address, "", "Address of the local machine");
DEFINE_string(data_dir, "", "Directory containing intial data");

using slog::Broker;
using slog::ConfigurationPtr;
using slog::MakeRunnerFor;

using std::make_shared;

int main(int argc, char* argv[]) {
  slog::InitializeService(&argc, &argv);

  // TODO - Add Zip Version

#if !defined(SYSTEM_GLOG) && !defined(SYSTEM_GEOZIP)
  LOG(ERROR) << "Order requires either GLOG or GEOZIP to be active";
  return -1;
#endif

  LOG(INFO) << "ZIP version: " << SLOG_VERSION;
  auto zmq_version = zmq::version();
  LOG(INFO) << "ZMQ version " << std::get<0>(zmq_version) << "." << std::get<1>(zmq_version) << "."
            << std::get<2>(zmq_version);


  CHECK(!FLAGS_address.empty()) << "Address must not be empty";
  auto config = slog::Configuration::FromFile(FLAGS_config, FLAGS_address);

  INIT_RECORDING(config);
  auto broker = Broker::New(config);

  auto config_name = FLAGS_config;
  if (auto pos = config_name.rfind('/'); pos != std::string::npos) {
    config_name = config_name.substr(pos + 1);
  }
  auto metrics_manager = make_shared<slog::MetricsRepositoryManager>(config_name, config);

  std::unique_ptr<slog::ModuleRunner> clock_sync;
  if (config->clock_synchronizer_port() != 0) {
    // Start the clock synchronizer early so that it runs while data is generating
    clock_sync = MakeRunnerFor<slog::ClockSynchronizer>(broker->context(), broker->config(), metrics_manager);
    clock_sync->StartInNewThread();
  }
  std::vector<std::pair<std::unique_ptr<slog::ModuleRunner>, slog::ModuleId>> modules;

#ifdef SYSTEM_GLOG
  LOG(INFO) << "GLOG Order: " << SLOG_VERSION;

  modules.emplace_back(MakeRunnerFor<glog::GLOGOrder>(broker->context(), broker->config(), metrics_manager),
                       slog::ModuleId::ZIP_ORDER);
#elif SYSTEM_GEOZIP
  LOG(INFO) << "Zip Order: " << SLOG_VERSION;

  modules.emplace_back(MakeRunnerFor<ziplog::Order>(broker, metrics_manager),
                       slog::ModuleId::ZIP_ORDER);
#endif

  // Block SIGINT from here so that the new threads inherit the block mask
  sigset_t signal_set;
  sigemptyset(&signal_set);
  sigaddset(&signal_set, SIGINT);
  pthread_sigmask(SIG_BLOCK, &signal_set, nullptr);

  // New modules cannot be bound to the broker after it starts so start
  // the Broker only after it is used to initialized all modules above.
  for (auto& [module, id] : modules) {
    std::optional<uint32_t> cpu;
    if (auto cpus = config->cpu_pinnings(id); !cpus.empty()) {
      cpu = cpus.front();
    }
    module->StartInNewThread(cpu);
  }

  // Suspense this thread until receiving SIGINT
  int sig;
  sigwait(&signal_set, &sig);

  // Shutdown all threads
  for (auto& module : modules) {
    module.first->Stop();
  }
  broker->Stop();

  return 0;
}