#pragma once

#include <sstream>
#include <map>

namespace slog {

/**
 * This data structure is a log of items numbered consecutively in
 * an increasing order.
 *
 * Items are added in order due to the pre-assignment strategy of
 * GeoZip. Items can be removed from the log out of order, for example
 * due to clients finishing or closing multi-slots.
 */
template <typename T>
class SyncLog {
 public:
  SyncLog(uint32_t start_from = 0) : current_(start_from) {}

  void Insert(uint32_t position, const T& item) {
    if (position < current_) {
      throw std::runtime_error("Cannot add slots after having processed previous ones");
    }
    auto ret = log_.emplace(position, item);
    if (ret.second == false) {
      std::ostringstream os;
      os << "Log position " << position << " has already been taken";
      throw std::runtime_error(os.str());
    }
  }

  bool HasNext() const { return !log_.empty(); }

  const T& Peek() { return log_.begin()->second; }

  uint32_t CurrentSlot() {
    if (!log_.empty()){
      return log_.begin()->first;
    }
    else {
      return std::numeric_limits<uint32_t>::max();
    }
  }

  std::pair<uint32_t, T> Next() {
    if (!HasNext()) {
      throw std::runtime_error("Next item does not exist");
    }
    current_++;

    auto it = log_.begin();
    auto res = *it;
    log_.erase(it);
    return res;
  }

  void Remove(uint32_t slot){
    log_.erase(slot);
  }

  /* For debugging */
  size_t NumBufferredItems() const { return log_.size(); }

 private:
  std::map<uint32_t, T> log_;
  uint32_t current_;
};

}  // namespace slog