include(GoogleTest)
include(FetchContent)

FetchContent_Declare(googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG        release-1.12.1
)
FetchContent_GetProperties(googletest)
if(NOT googletest_POPULATED)
  message("Populating: googletest")
  FetchContent_Populate(googletest)
  add_subdirectory(${googletest_SOURCE_DIR} ${googletest_BINARY_DIR})
endif()

add_library(general-test-utils general_test_util.cpp)
target_link_libraries(general-test-utils PUBLIC slog-core)

add_library(test-utils test_utils.cpp general_test_util.cpp)
target_link_libraries(test-utils PUBLIC slog-core)

macro(add_slog_test TESTFILE)
  get_filename_component(TESTNAME ${TESTFILE} NAME_WE)
  add_executable(${TESTNAME} ${TESTFILE})
  target_link_libraries(${TESTNAME}
    PRIVATE
      gmock
      gtest
      gtest_main
      test-utils)
  gtest_discover_tests(${TESTNAME}
    PROPERTIES 
      RUN_SERIAL TRUE
      TIMEOUT    5)
endmacro()

add_slog_test(common/batch_log_test.cpp)
add_slog_test(common/concurrent_hash_map_test.cpp)
add_slog_test(common/rolling_window_test.cpp)
add_slog_test(common/string_utils_test.cpp)
add_slog_test(connection/broker_and_sender_test.cpp)
add_slog_test(connection/zmq_utils_test.cpp)
add_slog_test(e2e/e2e_test.cpp)
add_slog_test(execution/tpcc/table_test.cpp)
add_slog_test(execution/tpcc/transaction_test.cpp)
add_slog_test(execution/tpcc/partial_rep_test.cpp)

add_slog_test(module/forwarder_test.cpp)
add_slog_test(module/log_manager_test.cpp)
add_slog_test(module/server_test.cpp)
add_slog_test(module/scheduler_components/ddr_lock_manager_test.cpp)
add_slog_test(module/scheduler_components/rma_lock_manager_test.cpp)
add_slog_test(module/scheduler_test.cpp)
add_slog_test(module/sequencer_test.cpp)
add_slog_test(paxos/paxos_test.cpp)
add_slog_test(storage/mem_only_storage_test.cpp)
add_slog_test(module/workload_test.cpp)
add_slog_test(logs/multi_slot_log_test.cpp)
add_slog_test(logs/pessimistic_slot_log_test.cpp)
add_slog_test(logs/dm_slot_log_test.cpp)

add_slog_test(integration/forwarder_sequencer_test.cpp)
add_slog_test(integration/forwarder_log_manager_test.cpp)

add_library(glog-test-utils glog_test_utils.cpp test_utils.cpp general_test_util.cpp)
target_link_libraries(glog-test-utils PUBLIC slog-core)

macro(add_glog_test TESTFILE)
  get_filename_component(TESTNAME ${TESTFILE} NAME_WE)
  add_executable(${TESTNAME} ${TESTFILE})
  target_link_libraries(${TESTNAME}
          PRIVATE
          gmock
          gtest
          gtest_main
          glog-test-utils)
  gtest_discover_tests(${TESTNAME}
          PROPERTIES
          RUN_SERIAL TRUE
          TIMEOUT    5)
endmacro()

add_glog_test(module/glog_components/glog_stub_test.cpp)
add_glog_test(module/glog_components/glog_order_test.cpp)
add_glog_test(module/glog_components/glog_test.cpp)
add_glog_test(module/glog_components/glog_sequencer_test.cpp)
add_glog_test(module/glog_components/glog_forwarder_test.cpp)

add_glog_test(e2e/e2e_glog_test.cpp)

add_library(tiga-test-utils tiga_test_utils.cpp general_test_util.cpp test_utils.cpp)
target_link_libraries(tiga-test-utils PUBLIC slog-core)

macro(add_tiga_test TESTFILE)
  get_filename_component(TESTNAME ${TESTFILE} NAME_WE)
  add_executable(${TESTNAME} ${TESTFILE})
  target_link_libraries(${TESTNAME}
          PRIVATE
          gmock
          gtest
          gtest_main
          tiga-test-utils)
  gtest_discover_tests(${TESTNAME}
          PROPERTIES
          RUN_SERIAL TRUE
          TIMEOUT    5)
endmacro()

add_tiga_test(module/tiga_components/coordinator_test.cpp)
add_tiga_test(module/tiga_components/acceptor_test.cpp)
add_tiga_test(module/tiga_components/tiga_scheduler_test.cpp)
add_tiga_test(module/tiga_components/tiga_worker_test.cpp)
add_tiga_test(module/tiga_components/tiga_server_test.cpp)
add_tiga_test(integration/tiga_integration/scheduler_worker_test.cpp)
add_tiga_test(e2e/e2e_tiga_test.cpp)
