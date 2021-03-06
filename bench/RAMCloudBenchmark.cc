#include "RAMCloudBenchmark.h"
#include <unistd.h>

#ifdef NO_LOG
#define LOG(out, fmt, ...)
#else
#define LOG(out, fmt, ...) fprintf(out, fmt, ##__VA_ARGS__)
#endif

//#define QUERY(client, i, num_keys)
#ifndef QUERY
#define QUERY(client, i, num_keys) {\
  if (query_types[i % kThreadQueryCount] == 0) {\
    client->read(table_id_, &keys[i % keys.size()], 8, &get_res, NULL, NULL);\
    num_keys++;\
  } else if (query_types[i % kThreadQueryCount] == 1) {\
    std::vector<uint64_t> search_res;\
    Search(search_res, client, queries[i % queries.size()]);\
    num_keys += search_res.size();\
  } else if (query_types[i % kThreadQueryCount] == 2) {\
    RecordData& record = records[i % records.size()];\
    KeyInfo *keys = record.GetKeys(cur_key++);\
    char* value = record.GetValue();\
    client->write(table_id_, num_attributes_ + 1, keys, value, strlen(value), NULL, NULL, false);\
    num_keys++;\
  } else {\
    client->remove(table_id_, &keys[i % keys.size()], 8, NULL, NULL);\
    num_keys++;\
  }\
}
#endif

RAMCloudBench::RAMCloudBench(std::string& data_path, uint32_t num_attributes,
                             std::string& hostname) {
  char resolved_path[100];
  realpath(data_path.c_str(), resolved_path);
  data_path_ = resolved_path;
  hostname_ = hostname;
  num_attributes_ = num_attributes;

  init_load_keys_ = 0;
  uint64_t b_size = 1000;

  int64_t cur_key = 0;

  RamCloud* client = NewClient();
  LOG(stderr, "Creating table...\n");
  table_id_ = client->createTable("table");
  LOG(stderr, "Creating indexes...\n");
  for (uint32_t i = 1; i <= num_attributes_; i++)
    client->createIndex(table_id_, i, 0, 1);

  LOG(stderr, "Starting to load data...\n");
  std::ifstream in(data_path);
  std::string cur_value;
  KeyInfo* keys = new KeyInfo[num_attributes + 1];
  while (std::getline(in, cur_value)) {
    uint64_t k = cur_key++;
    keys[0] = {&k, sizeof(uint64_t)};
    std::stringstream ss(cur_value);

    uint8_t key_id = 0;
    std::vector<std::string> key_buffer;
    while (key_id < num_attributes) {
      std::string key;
      std::getline(ss, key, '|');
      key_buffer.push_back(key);
      KeyInfo elem = {key_buffer[key_id].c_str(), key.length()};
      keys[key_id + 1] = elem;
      key_id++;

    }
    client->write(table_id_, num_attributes_ + 1, keys, cur_value.c_str(),
        cur_value.length(), NULL, NULL, false);
    init_load_keys_++;
    key_buffer.clear();
  }

  delete[] keys;

  LOG(stderr, "Data loading complete, loaded %llu keys.\n", init_load_keys_);
  delete client;
}

void RAMCloudBench::BenchmarkGetLatency() {
  // Create client
  RamCloud* client = NewClient();

  // Generate queries
  LOG(stderr, "Generating queries...");
  std::vector<uint64_t> keys;

  for (int64_t i = 0; i < kWarmupCount + kMeasureCount; i++) {
    uint64_t key = rand() % init_load_keys_;
    keys.push_back(key);
  }

  LOG(stderr, "Done.\n");

  std::ofstream result_stream("latency_get");

  Buffer result;
  // Warmup
  LOG(stderr, "Warming up for %llu queries...\n", kWarmupCount);
  for (uint64_t i = 0; i < kWarmupCount; i++) {
    client->read(table_id_, &keys[i], 8, &result, NULL, NULL);
    // shard_->Get(result, keys[i]);
  }
  LOG(stderr, "Warmup complete.\n");

  // Measure
  LOG(stderr, "Measuring for %llu queries...\n", kMeasureCount);
  for (uint64_t i = kWarmupCount; i < kWarmupCount + kMeasureCount; i++) {
    auto t0 = high_resolution_clock::now();
    client->read(table_id_, &keys[i], 8, &result, NULL, NULL);
    // shard_->Get(result, keys[i]);
    auto t1 = high_resolution_clock::now();
    auto tdiff = duration_cast<nanoseconds>(t1 - t0).count();
    result_stream << result.size() << "\t" << tdiff << "\n";
  }
  LOG(stderr, "Measure complete.\n");
  result_stream.close();
  delete client;
}

void RAMCloudBench::BenchmarkSearchLatency() {
  // Create client
  RamCloud* client = NewClient();

  LOG(stderr, "Reading queries...");
  std::vector<SearchQuery> queries;

  std::ifstream in(data_path_ + ".queries");
  int attr_id;
  std::string attr_val, entry;
  while (queries.size() < kWarmupCount + kMeasureCount
      && std::getline(in, entry)) {
    std::stringstream ss(entry);
    ss >> attr_id >> attr_val;
    SearchQuery query = { attr_id, attr_val };
    queries.push_back(query);
  }

  size_t warmup_count = queries.size() / 10;
  size_t measure_count = queries.size() - warmup_count;

  LOG(stderr, "Done.\n");

  std::ofstream result_stream("latency_search");

  // Warmup
  LOG(stderr, "Warming up for %llu queries...\n", warmup_count);
  for (uint64_t i = 0; i < warmup_count; i++) {
    SearchQuery& query = queries[i % queries.size()];
    std::vector<uint64_t> results;
    Search(results, client, query);
    // shard_->Search(results, query);
  }
  LOG(stderr, "Warmup complete.\n");

  // Measure
  LOG(stderr, "Measuring for %llu queries...\n", measure_count);
  for (uint64_t i = warmup_count; i < warmup_count + measure_count; i++) {
    SearchQuery& query = queries[i % queries.size()];
    std::vector<uint64_t> results;
    auto t0 = high_resolution_clock::now();
    // shard_->Search(results, query);
    Search(results, client, query);
    auto t1 = high_resolution_clock::now();
    auto tdiff = duration_cast<nanoseconds>(t1 - t0).count();
    result_stream << results.size() << "\t" << tdiff << "\n";
  }
  LOG(stderr, "Measure complete.\n");
  result_stream.close();
  delete client;
}

void RAMCloudBench::BenchmarkAppendLatency() {
  // Create client
  RamCloud* client = NewClient();

  // Generate queries
  LOG(stderr, "Generating queries...");
  std::vector<RecordData> records;

  std::ifstream in(data_path_ + ".inserts");
  std::string cur_value;
  for (int64_t i = 0;
      i < kWarmupCount + kMeasureCount && std::getline(in, cur_value); i++) {
    RecordData record(cur_value, num_attributes_);
    records.push_back(record);
  }
  int64_t cur_key = init_load_keys_;

  LOG(stderr, "Done.\n");

  std::ofstream result_stream("latency_append");

  // Warmup
  LOG(stderr, "Warming up for %llu queries...\n", kWarmupCount);
  for (uint64_t i = 0; i < kWarmupCount; i++) {
    RecordData& record = records[i];
    KeyInfo *keys = record.GetKeys(cur_key++);
    char* value = record.GetValue();
    client->write(table_id_, num_attributes_ + 1, keys, value, strlen(value),
    NULL,
                  NULL,
                  false);
  }
  LOG(stderr, "Warmup complete.\n");

  // Measure
  LOG(stderr, "Measuring for %llu queries...\n", kMeasureCount);
  for (uint64_t i = kWarmupCount; i < kWarmupCount + kMeasureCount; i++) {
    RecordData& record = records[i];
    KeyInfo *keys = record.GetKeys(cur_key++);
    char* value = record.GetValue();
    auto t0 = high_resolution_clock::now();
    client->write(table_id_, num_attributes_ + 1, keys, value, NULL, NULL,
    false);
    auto t1 = high_resolution_clock::now();
    auto tdiff = duration_cast<nanoseconds>(t1 - t0).count();
    cur_key++;
    result_stream << (cur_key - 1) << "\t" << tdiff << "\n";
  }
  LOG(stderr, "Measure complete.\n");
  result_stream.close();
  delete client;
}

void RAMCloudBench::BenchmarkDeleteLatency() {
  // Create client
  RamCloud* client = NewClient();

  // Generate queries
  LOG(stderr, "Generating queries...");
  std::vector<int64_t> keys;

  for (int64_t i = 0; i < kWarmupCount + kMeasureCount; i++) {
    int64_t key = rand() % init_load_keys_;
    keys.push_back(key);
  }

  LOG(stderr, "Done.\n");

  std::ofstream result_stream("latency_delete");

  // Warmup
  LOG(stderr, "Warming up for %llu queries...\n", kWarmupCount);
  for (uint64_t i = 0; i < kWarmupCount; i++) {
    client->remove(table_id_, &keys[i], 8, NULL, NULL);
    // shard_->Delete(keys[i]);
  }
  LOG(stderr, "Warmup complete.\n");

  // Measure
  LOG(stderr, "Measuring for %llu queries...\n", kMeasureCount);
  for (uint64_t i = kWarmupCount; i < kWarmupCount + kMeasureCount; i++) {
    auto t0 = high_resolution_clock::now();
    client->remove(table_id_, &keys[i], 8, NULL, NULL);
    // shard_->Delete(keys[i]);
    auto t1 = high_resolution_clock::now();
    auto tdiff = duration_cast<nanoseconds>(t1 - t0).count();
    result_stream << keys[i] << "\t" << tdiff << "\n";
  }
  LOG(stderr, "Measure complete.\n");
  result_stream.close();
  delete client;
}

void RAMCloudBench::BenchmarkThroughput(const double get_f,
                                        const double search_f,
                                        const double append_f,
                                        const double delete_f,
                                        const uint32_t num_clients) {

  if (get_f + search_f + append_f + delete_f != 1.0) {
    LOG(stderr, "Query fractions must add up to 1.0. Sum = %lf\n",
        get_f + search_f + append_f + delete_f);
    return;
  }

  const double get_m = get_f, search_m = get_f + search_f, append_m = get_f
      + search_f + append_f, delete_m = get_f + search_f + append_f + delete_f;

  std::condition_variable cvar;
  std::vector<std::thread> threads;
  std::atomic<uint64_t> cur_key;
  cur_key = init_load_keys_;

  Barrier barrier(num_clients);

  for (uint32_t i = 0; i < num_clients; i++) {
    threads.push_back(
        std::move(
            std::thread(
                [&] {
                  std::vector<int64_t> keys;
                  std::vector<RecordData> records;
                  std::vector<SearchQuery> queries;

                  std::ifstream in_s(data_path_ + ".queries");
                  std::ifstream in_a(data_path_ + ".inserts");

                  int attr_id;
                  std::string attr_val;
                  std::string value_str;
                  std::vector<uint32_t> query_types;
                  LOG(stderr, "Generating queries...\n");
                  for (int64_t i = 0; i < kThreadQueryCount; i++) {
                    int64_t key = RandomInteger(0, init_load_keys_);
                    std::string entry;
                    if (std::getline(in_s, entry)) {
                      std::stringstream ss(entry);
                      ss >> attr_id >> attr_val;
                      SearchQuery query = {attr_id, attr_val};
                      queries.push_back(query);
                    }
                    if (std::getline(in_a, value_str)) {
                      RecordData record(value_str, num_attributes_);
                      records.push_back(record);
                    }
                    keys.push_back(key);

                    double r = RandomDouble(0, 1);
                    if (r <= get_m) {
                      query_types.push_back(0);
                    } else if (r <= search_m) {
                      query_types.push_back(1);
                    } else if (r <= append_m) {
                      query_types.push_back(2);
                    } else if (r <= delete_m) {
                      query_types.push_back(3);
                    }
                  }

                  std::shuffle(keys.begin(), keys.end(), PRNG());
                  std::shuffle(queries.begin(), queries.end(), PRNG());
                  std::shuffle(records.begin(), records.end(), PRNG());
                  LOG(stderr, "Loaded %zu keys, %zu queries and %zu records.\n", keys.size(), queries.size(), records.size());

                  RamCloud* client = NewClient();

                  double query_thput = 0;
                  double key_thput = 0;
                  Buffer get_res;

                  barrier.Wait();
                  LOG(stderr, "Starting benchmark.\n");

                  try {
                    // Warmup phase
                    long i = 0;
                    long num_keys = 0;
                    TimeStamp warmup_start = GetTimestamp();
                    while (GetTimestamp() - warmup_start < kWarmupTime) {
                      QUERY(client, i, num_keys);
                      i++;
                    }

                    // Measure phase
                    i = 0;
                    num_keys = 0;
                    TimeStamp start = GetTimestamp();
                    while (GetTimestamp() - start < kMeasureTime) {
                      QUERY(client, i, num_keys);
                      i++;
                    }
                    TimeStamp end = GetTimestamp();
                    double totsecs = (double) (end - start) / (1000.0 * 1000.0);
                    query_thput = ((double) i / totsecs);
                    key_thput = ((double) num_keys / totsecs);

                    // Cooldown phase
                    i = 0;
                    num_keys = 0;
                    TimeStamp cooldown_start = GetTimestamp();
                    while (GetTimestamp() - cooldown_start < kCooldownTime) {
                      QUERY(client, i, num_keys);
                      i++;
                    }

                  } catch (std::exception &e) {
                    LOG(stderr, "Throughput thread ended prematurely.\n");
                  }

                  LOG(stderr, "Throughput: %lf\n", query_thput);

                  std::ofstream ofs;
                  char output_file[100];
                  sprintf(output_file, "throughput_%.2f_%.2f_%.2f_%.2f_%d", get_f, search_f, append_f, delete_f, num_clients);
                  ofs.open(output_file, std::ofstream::out | std::ofstream::app);
                  ofs << query_thput << "\t" << key_thput << "\n";
                  ofs.close();
                  delete client;
                })));
  }

  for (auto& th : threads) {
    th.join();
  }

}

void PrintUsage(char *exec) {
  LOG(stderr,
      "Usage: %s [-b bench-type] [-m mode] [-n num-clients] data-path\n", exec);
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
  if (argc < 2 || argc > 10) {
    PrintUsage(argv[0]);
    return -1;
  }

  int c;
  std::string bench_type = "latency-get", hostname = "localhost";
  int num_clients = 1;
  uint8_t num_attributes = 1;
  while ((c = getopt(argc, argv, "b:n:a:h:")) != -1) {
    switch (c) {
      case 'b':
        bench_type = std::string(optarg);
        break;
      case 'a':
        num_attributes = atoi(optarg);
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

  RAMCloudBench bench(data_path, num_attributes, hostname);
  if (bench_type == "latency-get") {
    bench.BenchmarkGetLatency();
  } else if (bench_type == "latency-search") {
    bench.BenchmarkSearchLatency();
  } else if (bench_type == "latency-append") {
    bench.BenchmarkAppendLatency();
  } else if (bench_type == "latency-delete") {
    bench.BenchmarkDeleteLatency();
  } else if (bench_type.find("throughput") == 0) {
    std::vector<std::string> tokens = Split(bench_type, '-');
    if (tokens.size() != 5) {
      LOG(stderr, "Error: Incorrect throughput benchmark format.\n");
      return -1;
    }
    double get_f = atof(tokens[1].c_str());
    double search_f = atof(tokens[2].c_str());
    double append_f = atof(tokens[3].c_str());
    double delete_f = atof(tokens[4].c_str());
    LOG(stderr,
        "get_f = %.2lf, search_f = %.2lf, append_f = %.2lf, delete_f = %.2lf, num_clients = %d\n",
        get_f, search_f, append_f, delete_f, num_clients);
    bench.BenchmarkThroughput(get_f, search_f, append_f, delete_f, num_clients);
  } else {
    LOG(stderr, "Unknown benchmark type: %s\n", bench_type.c_str());
  }

  return 0;
}
