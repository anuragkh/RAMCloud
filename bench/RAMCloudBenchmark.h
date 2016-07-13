#ifndef RAMCLOUDBENCHMARK_H_
#define RAMCLOUDBENCHMARK_H_

#include <chrono>
#include <sys/time.h>
#include <random>
#include <fstream>
#include <sstream>
#include <cstring>
#include <vector>
#include <string>

#include "RamCloud.h"
#include "IndexKey.h"
#include "IndexLookup.h"

using namespace RAMCloud;
using namespace ::std::chrono;

class RAMCloudBench {
 public:
  typedef unsigned long long int TimeStamp;

  static const uint64_t kWarmupCount = 1000;
  static const uint64_t kMeasureCount = 100000;
  static const uint64_t kCooldownCount = 1000;

  static const uint64_t kWarmupTime = 5000000;
  static const uint64_t kMeasureTime = 10000000;
  static const uint64_t kCooldownTime = 5000000;

  static const uint64_t kThreadQueryCount = 75000;

  struct RecordData {
    RecordData(std::string value_str, uint8_t num_attributes,
               char delim = '|') {
      keys = new KeyInfo[num_attributes + 1];
      std::stringstream ss(value_str);
      std::string key;
      uint8_t key_id = 0;
      keys[0] = {NULL, sizeof(uint64_t)};
      while (std::getline(ss, key, delim) && key_id < num_attributes) {
        char *keyptr = strdup(key.c_str());
        KeyInfo elem = { keyptr, 0 };
        keys[key_id + 1] = elem;
        key_id++;
      }

//      fprintf(stderr, "Keys: (");
//      for (size_t i = 0; i < num_attributes; i++)
//        fprintf(stderr, "%s, ", keys[i + 1].key);
//      fprintf(stderr, ") Value: %s\n", value);

      value = strdup(value_str.c_str());
    }

    KeyInfo* GetKeys(uint64_t primary_key) {
      keys[0].key = &primary_key;
      return keys;
    }

    char* GetValue() {
      return value;
    }

   private:
    KeyInfo* keys;
    char* value;
  };

  struct SearchQuery {
    uint8_t attr_id;
    std::string attr_val;
  };

  RAMCloudBench(std::string& data_path, uint32_t num_attributes);

  // Latency benchmarks
  void BenchmarkGetLatency();
  void BenchmarkSearchLatency();
  void BenchmarkAppendLatency();
  void BenchmarkDeleteLatency();

  // Throughput benchmarks
  void BenchmarkThroughput(const double get_f, const double search_f,
                           const double append_f, const double delete_f,
                           const uint32_t num_clients = 1);

  RamCloud* NewClient() {
    return new RamCloud("tcp:host=0.0.0.0,port=11211", "main");
  }

  void Search(std::vector<uint64_t>& keys, RamCloud* client,
              SearchQuery& query) {
    IndexKey::IndexKeyRange range(query.attr_id, query.attr_val.c_str(),
                                  query.attr_val.length(),
                                  query.attr_val.c_str(),
                                  query.attr_val.length());
    IndexLookup indexLookup(client, table_id_, range);
    while (indexLookup.getNext()) {
      uint16_t pKeyLen;
      uint64_t pKey = *(uint64_t *) (indexLookup.currentObject()->getKey(
          0, &pKeyLen));
      keys.push_back(pKey);
    }
  }

 private:
  static TimeStamp GetTimestamp() {
    struct timeval now;
    gettimeofday(&now, NULL);

    return now.tv_usec + (TimeStamp) now.tv_sec * 1000000;
  }

  static uint32_t RandomInteger(const uint32_t min, const uint32_t max) {
    std::random_device r;
    std::seed_seq seed { r(), r(), r(), r(), r(), r(), r(), r() };
    static thread_local std::mt19937 generator(seed);
    std::uniform_int_distribution<uint32_t> distribution(min, max);
    return distribution(generator);
  }

  static uint32_t RandomIndex(const uint32_t i) {
    return RandomInteger(0, i);
  }

  static std::mt19937 PRNG() {
    std::random_device r;
    std::seed_seq seed { r(), r(), r(), r(), r(), r(), r(), r() };
    return std::mt19937(seed);
  }

  static double RandomDouble(const double min, const double max) {
    std::random_device r;
    std::seed_seq seed { r(), r(), r(), r(), r(), r(), r(), r() };
    static thread_local std::mt19937 generator(seed);
    std::uniform_real_distribution<double> distribution(min, max);
    return distribution(generator);
  }

  std::string data_path_;
  uint8_t num_attributes_;
  uint64_t init_load_bytes_;
  uint64_t init_load_keys_;
  uint64_t table_id_;
};

#endif /* RAMCLOUDBENCHMARK_H_ */
