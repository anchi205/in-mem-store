#pragma once
// #include "leanstore/Config.hpp"
#include <string>
#include <cstdint>
#include <fstream>

namespace leanstore {
namespace storage {
namespace inmem {

// WAL entry format: LSN | Namespace ID | Payload Size | Payload | CRC32
struct memWalWrite {
   uint64_t lsn;           // Log sequence number
   uint32_t namespace_id;  // Transaction namespace identifier
   uint8_t payload_size;  // Size of the payload in bytes
   uint32_t* payload;       // Variable length payload data
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
  // std::string currentMemWALFile = "wal-1.txt"

  MemWALManager();
  // set config here in this method

  void writeMemWALEntry(const std::string& ns_str);
  void recoverMemWALEntries(); // takes array of namespaces
  void closeMemWAL();


private:
  void changeWALFile();
  void rotateSegment();
  void deleteOldSegment();
  void SyncBuffer();

   std::ofstream wal_file;
};

} // namespace inmem
} // namespace storage
} // namespace leanstore
