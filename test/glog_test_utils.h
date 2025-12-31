//
// Created by jrsoares on 31-03-2025.
//

#ifndef SLOG_GLOG_TEST_UTILS_H
#define SLOG_GLOG_TEST_UTILS_H

#include "test_utils.h"

/**
 * This is a fake GLOG system where we can only add a subset
 * of modules to test them in isolation.
 */

namespace glog {

using namespace slog;

using ConfigVec = std::vector<ConfigurationPtr>;

ConfigVec MakeGlogTestConfigurations(string&& prefix, int num_regions, int num_replicas, int num_partitions,
                                     internal::Configuration common_config = {}, bool local_sync_rep = false);

class TestGlog : public TestSlog {
 public:
  TestGlog(const ConfigurationPtr& config, bool init = false);
  void AddOrder();
  void AddGlogStubs(int num_glog_stubs, bool noop_responds = false);
  void AddGlog();
  //void SendGlogNewEpoch(string& address);

  void StartGlogInNewThreads();
  void AddGlogOutputSocket(Channel channel, const std::vector<uint64_t>& tags = {});
  milliseconds GetEpochDuration();
 private:
  ModuleRunnerPtr glog_;
  ModuleRunnerPtr order_;

  std::vector<ModuleRunnerPtr> glog_stubs_;

  bool order_exists_ = false;
};

}

#endif  // SLOG_GLOG_TEST_UTILS_H
