#include "PacketLoader.h"
#include <unistd.h>

#ifdef NO_LOG
#define LOG(out, fmt, ...)
#else
#define LOG(out, fmt, ...) fprintf(out, fmt, ##__VA_ARGS__)
#endif

PacketLoader::PacketLoader(std::string& data_path, std::string& attr_path,
                           std::string& hostname) {
  char resolved_path[100];
  realpath(data_path.c_str(), resolved_path);
  data_path_ = std::string(resolved_path);
  realpath(attr_path.c_str(), resolved_path);
  attr_path_ = std::string(resolved_path);
  hostname_ = hostname;

  RamCloud* client = NewClient();

  LOG(stderr, "Creating table...\n");
  table_id_ = client->createTable("table");

  LOG(stderr, "Creating indexes...\n");
  for (uint32_t i = 1; i <= 5; i++)
    client->createIndex(table_id_, i, 0, 1);

  LOG(stderr, "Loading data...\n");
  LoadData();

  LOG(stderr, "Initialization complete.\n");
  delete client;
}

void PacketLoader::LoadData() {
  std::ifstream ind(data_path_);
  std::ifstream ina(attr_path_);
  std::string attr_line;
  LOG(stderr, "Reading from path data=%s, attr=%s\n", data_path_.c_str(),
      attr_path_.c_str());

  while (std::getline(ina, attr_line)) {
    std::stringstream attr_stream(attr_line);
    std::string ts, srcip, dstip, sport, dport;
    uint16_t len;
    attr_stream >> ts >> len >> srcip >> dstip >> sport >> dport;
    char* data = new char[len];
    ind.read(data, len);
    timestamps_.push_back(ts);
    srcips_.push_back(srcip);
    dstips_.push_back(dstip);
    sports_.push_back(sport);
    dports_.push_back(dport);
    datas_.push_back(data);
    datalens_.push_back(len);
  }

  LOG(stderr, "Loaded %zu packets.\n", datas_.size());
}

void PacketLoader::LoadPackets(const uint32_t num_clients, const uint64_t timebound) {

  std::condition_variable cvar;
  std::vector<std::thread> threads;

  Barrier barrier(num_clients);

  std::ofstream rfs;
  rfs.open("record_progress");

  std::mutex report_mtx;
  for (uint32_t i = 0; i < num_clients; i++) {
    threads.push_back(std::move(std::thread([&] {
      RamCloud* client = NewClient();
      double throughput = 0;

      barrier.Wait();

      LOG(stderr, "Starting benchmark.\n");

      try {
        int64_t local_ops = 0;
        int64_t total_ops = 0;

        TimeStamp start = GetTimestamp();
        while (total_ops >= 0 && GetTimestamp() - start < timebound) {
          total_ops = InsertPacket(client);
          local_ops += (total_ops > 0);
          if (total_ops % kReportRecordInterval == 0) {
            std::lock_guard<std::mutex> lock(report_mtx);
            rfs << GetTimestamp() << "\t" << total_ops << "\n";
          }
        }
        TimeStamp end = GetTimestamp();
        double totsecs = (double) (end - start) / (1000.0 * 1000.0);
        throughput = ((double) local_ops / totsecs);
      } catch (std::exception &e) {
        LOG(stderr, "Throughput thread ended prematurely.\n");
      }

      LOG(stderr, "Throughput: %lf\n", throughput);

      std::ofstream ofs;
      char output_file[100];
      ofs.open("write_throughput.txt", std::ofstream::out | std::ofstream::app);
      ofs << throughput << "\n";
      ofs.close();
      delete client;
    })));
  }

  for (auto& th : threads) {
    th.join();
  }

}

void PrintUsage(char *exec) {
  LOG(stderr, "Usage: %s -h [hostname] -n [numthreads] [data] [attrs]\n", exec);
}

std::vector<std::string> &Split(const std::string &s, char delim,
                                std::vector<std::string> &elems) {
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, delim)) {
    elems.push_back(item);
  }
  return elems;
}

std::vector<std::string> Split(const std::string &s, char delim) {
  std::vector<std::string> elems;
  Split(s, delim, elems);
  return elems;
}

int main(int argc, char** argv) {
  if (argc < 3 || argc > 9) {
    PrintUsage(argv[0]);
    return -1;
  }

  int c;
  std::string bench_type = "latency-get", hostname = "localhost";
  int num_clients = 1;
  uint8_t num_attributes = 1;
  uint64_t timebound = UINT64_MAX;
  while ((c = getopt(argc, argv, "t:n:h:")) != -1) {
    switch (c) {
      case 't':
        timebound = atol(optarg) * 10e6;
        break;
      case 'n':
        num_clients = atoi(optarg);
        break;
      case 'h':
        hostname = std::string(optarg);
        break;
      default:
        LOG(stderr, "Could not parse command line arguments.\n");
    }
  }

  if (optind == argc) {
    PrintUsage(argv[0]);
    return -1;
  }

  std::string data_path = std::string(argv[optind]);
  std::string attr_path = std::string(argv[optind + 1]);

  PacketLoader loader(data_path, attr_path, hostname);
  loader.LoadPackets(num_clients, timebound);

  return 0;
}
