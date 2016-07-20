#include <cstdio>
#include <sstream>
#include <cstring>
#include <vector>
#include <string>
#include <fstream>
#include <cstdint>
#include "RamCloud.h"

using namespace RAMCloud;

int main(int argc, char** argv) {
  if (argc < 2 || argc > 6) {
    fprintf(stderr, "Usage: %s -h [hostname] [filename]\n", argv[0]);
    return -1;
  }

  int c;
  std::string hostname = "localhost";
  uint8_t num_attributes = 1;
  while ((c = getopt(argc, argv, "a:h:")) != -1) {
    switch (c) {
      case 'a':
        num_attributes = atoi(optarg);
        break;
      case 'h':
        hostname = std::string(optarg);
        break;
      default:
        fprintf(stderr, "Could not parse command line arguments.\n");
    }
  }

  if (optind == argc) {
    fprintf(stderr, "Usage: %s -h [hostname] [filename]\n", argv[0]);
    return -1;
  }

  char* data_path = argv[optind];

  char connector[256];
  sprintf(connector, "tcp:host=%s,port=11211", hostname);
  fprintf(stderr, "Connecting to server; connector = %s\n", connector);
  RamCloud* client = new RamCloud(connector, "main");

  char resolved_path[100];
  realpath(data_path, resolved_path);

  uint64_t init_load_keys = 0;
  uint64_t b_size = 1000;

  int64_t cur_key = 0;

  fprintf(stderr, "Creating table...\n");
  uint64_t table_id = client->createTable("table");
  fprintf(stderr, "Creating indexes...\n");
  for (uint32_t i = 1; i <= num_attributes; i++)
    client->createIndex(table_id, i, 0, 1);

  fprintf(stderr, "Starting to load data...\n");
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
      KeyInfo elem = { key_buffer[key_id].c_str(), key.length() };
      keys[key_id + 1] = elem;
      key_id++;

    }
    client->write(table_id, num_attributes + 1, keys, cur_value.c_str(),
                  cur_value.length(), NULL, NULL, false);
    init_load_keys++;
    key_buffer.clear();
  }

  delete[] keys;

  fprintf(stderr, "Data loading complete, loaded %llu keys.\n", init_load_keys);
  delete client;

  return 0;
}

