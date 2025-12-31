//
// Created by jrsoares on 31-03-2025.
//

#ifndef SLOG_STUB_UTILS_H
#define SLOG_STUB_UTILS_H

namespace slog {

inline unsigned long align(unsigned long value, unsigned long min_slots){
  return (value/min_slots + 1)*min_slots;
}

}


#endif  // SLOG_STUB_UTILS_H
