#pragma once

#include "common/constants.h"

namespace slog {

class Bitmap {
 public:
  Bitmap(uint32_t rows, uint32_t cols)
      : rows_(rows), cols_(cols),
        data_((rows * cols + 7) / 8, 0) {}   // allocate enough bytes

  uint32_t rows() const { return rows_; }
  uint32_t cols() const { return cols_; }

  // Set pixel at (r, c) to 0 or 1
  void set(uint32_t r, uint32_t c, bool value) {
    check_bounds(r, c);
    uint32_t bitIndex = r * cols_ + c;
    uint32_t byteIndex = bitIndex >> 3;     // divide by 8
    uint8_t bitMask   = 1u << (bitIndex & 7); // modulo 8

    if (value)
      data_[byteIndex] |= bitMask;
    else
      data_[byteIndex] &= ~bitMask;
  }

  // Read pixel at (r, c)
  bool get(uint32_t r, uint32_t c) const {
    check_bounds(r, c);
    uint32_t bitIndex = r * cols_ + c;
    uint32_t byteIndex = bitIndex >> 3;
    uint8_t bitMask   = 1u << (bitIndex & 7);
    return (data_[byteIndex] & bitMask) != 0;
  }

  // Raw packed bytes
  const std::vector<uint8_t>& raw() const {
    return data_;
  }

 private:
  uint32_t rows_, cols_;
  std::vector<uint8_t> data_;

  void check_bounds(uint32_t r, uint32_t c) const {
    if (r >= rows_ || c >= cols_) {
      throw std::out_of_range("Bitmap index out of range");
    }
  }
};

inline void setBit(uint64_t &bitmap, int line, int col) {
  if (line < 0 || line >= slog::JANUS_MAX_NUM_REGIONS || col < 0 || col >= slog::JANUS_MAX_NUM_PARTITIONS) {
    throw std::out_of_range("line/col out of range");
  }

  int bitIndex = line * slog::JANUS_MAX_NUM_PARTITIONS + col; // row-major order
  bitmap |= (1ULL << bitIndex);
}

inline bool isBitSet(uint64_t bitmap, int line, int col) {
  if (line < 0 || line >= slog::JANUS_MAX_NUM_REGIONS || col < 0 || col >= slog::JANUS_MAX_NUM_PARTITIONS) {
    throw std::out_of_range("line/col out of range");
  }
  int bitIndex = line * slog::JANUS_MAX_NUM_REGIONS + col;
  return bitmap & (1ULL << bitIndex);
}
}
