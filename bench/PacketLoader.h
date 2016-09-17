#ifndef PACKETLOADER_H_
#define PACKETLOADER_H_

#include <chrono>
#include <sys/time.h>
#include <random>
#include <fstream>
#include <sstream>
#include <cstring>
#include <vector>
#include <string>
#include <atomic>

#include "RamCloud.h"
#include "IndexKey.h"
#include "IndexLookup.h"

using namespace RAMCloud;
using namespace ::std::chrono;

class PacketLoader {
 public:
  typedef unsigned long long int TimeStamp;
  static const uint64_t kMeasureTime = 30000000;

  int64_t InsertPacket(RamCloud* client) {

    uint64_t cur_id = id.fetch_add(1L);
    if (cur_id < timestamps_.size()) {
      return -1L;
    }

    KeyInfo keys_buf[6];
    keys_buf[0] = {&cur_id, sizeof(uint64_t)};
    keys_buf[1] = {timestamps_[cur_id].c_str(), timestamps_[cur_id].length()};
    keys_buf[2] = {srcips_[cur_id].c_str(), srcips_[cur_id].length()};
    keys_buf[3] = {dstips_[cur_id].c_str(), dstips_[cur_id].length()};
    keys_buf[4] = {sports_[cur_id].c_str(), sports_[cur_id].length()};
    keys_buf[5] = {dports_[cur_id].c_str(), dports_[cur_id].length()};

    client->write(table_id_, 6, keys_buf, datas_[cur_id], datalens_[cur_id],
                  NULL, NULL, false);
    return cur_id + 1;
  }

  class Barrier {
   public:
    explicit Barrier(std::size_t count)
        : count_ { count } {
    }
    void Wait() {
      std::unique_lock<std::mutex> lock { mutex_ };
      if (--count_ == 0) {
        cv_.notify_all();
      } else {
        cv_.wait(lock, [this] {return count_ == 0;});
      }
    }

   private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::size_t count_;
  };

  PacketLoader(std::string& data_path, std::string& attr_path,
               std::string& hostname);

  void LoadData();

  // Throughput benchmarks
  void LoadPackets(const uint32_t num_clients = 1);

  RamCloud* NewClient() {
    char connector[256];
    sprintf(connector, "tcp:host=%s,port=11211", hostname_.c_str());
    fprintf(stderr, "Connecting to server; connector = %s\n", connector);
    return new RamCloud(connector, "main");
  }

 private:
  static TimeStamp GetTimestamp() {
    struct timeval now;
    gettimeofday(&now, NULL);

    return now.tv_usec + (TimeStamp) now.tv_sec * 1000000;
  }

  std::string data_path_;
  std::string attr_path_;
  std::string hostname_;

  std::vector<std::string> timestamps_;
  std::vector<std::string> srcips_;
  std::vector<std::string> dstips_;
  std::vector<std::string> sports_;
  std::vector<std::string> dports_;
  std::vector<char*> datas_;
  std::vector<uint16_t> datalens_;

  uint64_t table_id_;
  std::atomic<uint64_t> id;
};

#endif /* RAMCLOUDBENCHMARK_H_ */
