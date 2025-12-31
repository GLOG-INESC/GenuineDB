//
// Created by jrsoares on 01/10/25.
//

#include "common/configuration.h"
#include "common/types.h"
#include <random>
#include <iostream>
#include <fstream>
#include <stdexcept>
#include <filesystem>

namespace slog {


static void saveZipfVector(const std::vector<double>& vec, const std::string& filename) {
  std::ofstream out(filename, std::ios::binary);
  if (!out) throw std::runtime_error("Could not open file for writing");

  // Save the size
  size_t size = vec.size();
  out.write(reinterpret_cast<const char*>(&size), sizeof(size));

  // Save the raw data
  out.write(reinterpret_cast<const char*>(vec.data()), size * sizeof(double));

}

static std::vector<double> loadZipfVector(const std::string& filename) {
  // Check if file exists
  if (!std::filesystem::exists(filename)) {
    std::cerr << "File not found: " << filename << "\n";
    return {}; // Nothing loaded
  }

  std::ifstream in(filename, std::ios::binary);
  if (!in) throw std::runtime_error("Could not open file for reading");

  size_t size;
  in.read(reinterpret_cast<char*>(&size), sizeof(size));

  std::vector<double> vec(size);
  in.read(reinterpret_cast<char*>(vec.data()), size * sizeof(double));
  return vec;
}

// Generic Access Distribution function
class AccessDistribution {
 public:
  virtual ~AccessDistribution() = default;
  virtual uint64_t GetKey(std::mt19937& rg) = 0;

};

class UniformDistribution : public AccessDistribution {
 public:
  UniformDistribution(const uint64_t num_keys) : dis_(0, num_keys-1) {

  }

  uint64_t GetKey(std::mt19937& rg) override {
    return dis_(rg);
  }

 private:
  std::uniform_int_distribution<uint64_t> dis_;
};

class ZipfDistribution : public AccessDistribution {
 public:
  // Default constructor
  ZipfDistribution(){};

  ZipfDistribution(const uint64_t num_keys, const double zipf) {
    Initialize(num_keys, zipf);
  }

  bool IsInitialized(){
    return initialized_;
  }
  void Initialize(const uint64_t num_keys, const double zipf){
    std::cout << "INIT" << std::endl;
    std::vector<double> weights(num_keys);
    double s = 0;
    for (size_t i = 1; i <= num_keys; i++) {
      s += 1 / std::pow(i, zipf);
    }
    for (size_t i = 0; i < num_keys; i++) {
      weights[i] = 1 / (std::pow(i + 1, zipf) * s);
    }

    zipf_dist_ = std::discrete_distribution<>(weights.begin(), weights.end());

    initialized_ = true;

  }

  uint64_t GetKey(std::mt19937& rg) override {

    return zipf_dist_(rg);
  }

 private:
  std::discrete_distribution<> zipf_dist_;

  std::vector<double> weights_;

  bool initialized_ = false;
};
/*

class ZipfDistribution : public AccessDistribution {
 public:
  ZipfDistribution(uint64_t num_keys, const double zipf) : num_keys_(num_keys), dist_(0.0, 1.0) {
    for (int i = 0; i <= num_keys; i++) {
      c_ = c_ + (1.0 / pow((double)i, zipf));
    }

    c_ = 1.0 / c_;

    std::ostringstream file_name;

    file_name << "/tmp/" << num_keys << "_" << zipf << ".bin";

    sum_probs_ = loadArray(num_keys, file_name.str());

    if (sum_probs_ == nullptr){
      sum_probs_ = new double[num_keys + 1];
      sum_probs_[0] = 0;
      for (int i = 1; i <= num_keys; i++) {
        sum_probs_[i] = sum_probs_[i - 1] + c_ / pow((double)i, zipf);
      }

      saveArray(sum_probs_, num_keys, file_name.str());
    }


  }

  uint64_t GetKey(std::mt19937& rg) override {
    // Uniform random number (0 < z < 1)
    double z;

    do
    {
      z = dist_(rg);
    }

    while ((z == 0) || (z == 1));

    // Map z to the value
    uint64_t low = 1;
    uint64_t high = num_keys_;
    uint64_t mid;
    uint64_t zipf_value;
    do {
      mid = floor((low+high)/2);
      if (sum_probs_[mid] >= z && sum_probs_[mid-1] < z) {
        zipf_value = mid;
        break;
      } else if (sum_probs_[mid] >= z) {
        high = mid-1;
      } else {
        low = mid+1;
      }
    } while (low <= high);

    // Assert that zipf_value is between 1 and N
    assert((zipf_value >=1) && (zipf_value <= num_keys_));

    return(zipf_value-1);
  }

 private:
  // Pre-calculated sum of probabilities
  double* sum_probs_;

  // Normalization constant
  double c_ = 0;

  uint64_t num_keys_;

  std::uniform_real_distribution<> dist_;
};
*/

}
