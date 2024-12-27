#pragma once
// #include "leanstore/Config.hpp"
#include <string>
#include <cstdint>
#include <fstream>
#include <mutex>
#include <thread>
#include <vector>
#include <atomic>
#include <algorithm>

namespace leanstore {
namespace storage {
namespace inmem {

// WAL entry format: LSN | Namespace ID | Payload Size | Payload | CRC32
struct memWalWrite {
  char payload[16];
  uint64_t lsn;           // Log sequence number
  uint32_t namespace_id;  // Transaction namespace identifier
  uint32_t crc;          // CRC32 checksum of the entry
};


struct MemWALConfig {
  std::string segment_prefix = "seg-";
  std::string wal_path = "/tmp/memwal";
  uint64_t wal_buffer_size = 1024 * 1024; // 1MB buffer
  uint64_t wal_segment_size = 64 * 1024 * 1024; // 64MB segments
  int wal_buffer_sync_ms = 200;
};


class MemWALManager {
public:
  
  MemWALConfig config;
  uint64_t MemWALlsn = 0;
  int currentSegment;

  
  // set config here in this method

  void writeMemWALEntry(const std::string& ns_str);
  void recoverMemWALEntries(); // takes array of namespaces
  void closeMemWAL();
  MemWALManager();
  
  // std::atomic<bool> running;
  // std::thread ticker_thread;
  // std::mutex buffer_mutex;


private:
  static const int BUFFER_SIZE = 16 * 1024 * 1024; // 16MB
  std::vector<memWalWrite> wal_buffer;  
  
  // in init function
  int max_wal_buffer_size;


  // statys same for now
  std::string filename;

  void changeWALFile();
  void rotateSegment();
  void deleteOldSegment();
  void SyncBuffer();
  std::string structureWALEntry(memWalWrite en);
  uint64_t getCurrentLSN() const {
    return MemWALlsn;
  }

  uint64_t incrementLSN() {
    return MemWALlsn++;
  }

  std::ofstream wal_file;

  void ticker_routine() {
        // while (running) {
        //     std::this_thread::sleep_for(std::chrono::seconds(1));
        //     flush_buffer();
        // }
    }

  void flush_buffer() {
      if (!wal_file.is_open() || wal_buffer.empty()) {
          return;
      }
      std::vector<std::string> strs;
      strs.resize(wal_buffer.size());
      
      std::transform(wal_buffer.begin(), wal_buffer.end(), strs.begin(),
          [this](const memWalWrite& entry) { return this->structureWALEntry(entry); });

      wal_buffer.clear();
      wal_buffer.resize(0);

    std::string filename = "/home/ayush/Documents/in-mem-store/memwal/seg-1.wal";
    std::ofstream outfile;
    outfile.open(filename, std::ios::app); // Append mode
    if (outfile.is_open()) {
      for(int i = 0 ; i < strs.size(); i++) {
        outfile << strs[i] << "\n" ;
      }
        
        outfile.close();
    }

  }

    

  void force_flush() {
        // flush_buffer();
    }
};

} // namespace inmem
} // namespace storage
} // namespace leanstore

// Write namespace to file in parent directory
// void writeNamespaceToFile(const std::string& ns_str) {
//    std::string filename = "/home/ayush/Documents/in-mem-store/namespace_log.txt";
//    std::ofstream outfile;
//    outfile.open(filename, std::ios::app); // Append mode
//    if (outfile.is_open()) {
//       outfile << ns_str << std::endl;
//       outfile.close();
//    }
// }
