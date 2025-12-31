//
// Created by jrsoares on 17/08/25.
//

#pragma once

#include "common/constants.h"
#include "common/types.h"
#include <glog/logging.h>

namespace slog {

using namespace std;

static inline void* AllocateHugePage(unsigned long size) {
  auto ptr = std::aligned_alloc(slog::HUGE_PAGE_SIZE, size);
  CHECK(ptr != nullptr) << " Cloud not allocate new memory page";
  return ptr;
}
}