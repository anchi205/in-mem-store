// aof.hpp
#ifndef AOF_HPP
#define AOF_HPP

#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <mutex>
#include <fstream>
#include <filesystem>
#include <sstream>
#include <chrono>
#include <zlib.h>
#include <functional>
#include <map>
#include "../../../../shared-headers/Units.hpp"  // For u8, u16 types

namespace fs = std::filesystem;

const std::string SEGMENT_PREFIX = "segment_";

enum RetentionMode {
    RETENTION_MODE_TIME,
    RETENTION_MODE_COUNT
};

enum RotationMode {
    ROTATION_MODE_TIME,
    ROTATION_MODE_SIZE
};

enum WALRecordType {
    INSERT = 1,
    UPDATE = 2,
    REMOVE = 3
};

struct WALEntry {
    uint8_t Version;
    uint64_t SequenceNo;
    uint64_t NamespaceId;
    std::vector<uint8_t> Data;  // Using vector for dynamic allocation
    uint32_t CRC32;
    uint64_t Timestamp;

    WALEntry() : Version(0), SequenceNo(0), NamespaceId(0), CRC32(0), Timestamp(0) {
        Data.reserve(1024);  // Pre-allocate reasonable size
    }
    
    void clear() {
        Data.clear();
        Data.shrink_to_fit();
    }
};

// Callback type for replaying operations
using ReplayCallback = std::function<void(uint64_t namespace_id, WALRecordType type, const u8* key, u16 key_length, const u8* value, u16 value_length)>;

class AOF {
public:
    AOF(const std::string& directory);
    ~AOF();

    bool Init();
    bool LogCommand(uint64_t namespace_id, const std::vector<uint8_t>& data);
    bool getStop() { return stop; }
    void setStop(bool stop){
        this->stop = stop;
    };

    // Recovery methods
    bool StartRecovery(const ReplayCallback& callback);
    bool StartNamespacesRecovery(const ReplayCallback& callback, std::vector<u64> ns_id);
    uint64_t GetLastSequenceNumber() const { return lastSequenceNo; }
    std::vector<uint64_t> GetNamespaces() const;
    bool ReplayNamespace(uint64_t namespace_id, const ReplayCallback& callback);
    std::vector<u8> serialize_for_wal(const u8* key, u16 key_length, const u8* value, u16 value_length) {
      std::vector<u8> log_data;
      log_data.reserve(sizeof(u16) + key_length + sizeof(u16) + value_length);
      // Add key length and key
      log_data.insert(log_data.end(), reinterpret_cast<const u8*>(&key_length), reinterpret_cast<const u8*>(&key_length) + sizeof(u16));
      log_data.insert(log_data.end(), key, key + key_length);
      // Add value length and value
      log_data.insert(log_data.end(), reinterpret_cast<const u8*>(&value_length), reinterpret_cast<const u8*>(&value_length) + sizeof(u16));
      log_data.insert(log_data.end(), value, value + value_length);
      return log_data;
   }

   std::tuple<std::vector<u8>, u16, std::vector<u8>, u16> deserialize_from_wal(const std::vector<u8>& log_data) {
      size_t pos = 0;
      // Read key length
      u16 key_length = *reinterpret_cast<const u16*>(log_data.data() + pos);
      pos += sizeof(u16);
      // Read key
      std::vector<u8> key(log_data.begin() + pos, log_data.begin() + pos + key_length);
      pos += key_length;
      // Read value length
      u16 value_length = *reinterpret_cast<const u16*>(log_data.data() + pos);
      pos += sizeof(u16);
      // Read value
      std::vector<u8> value(log_data.begin() + pos, log_data.begin() + pos + value_length);
      return {key, key_length, value, value_length};
   }
   std::vector<u8> serialize_for_log(WALRecordType type, const std::vector<u8>& data) {
      std::vector<u8> wal_data;
      wal_data.push_back(static_cast<u8>(type));  // First byte is the record type
      wal_data.insert(wal_data.end(), data.begin(), data.end());
      return wal_data;
   }
   std::pair<WALRecordType, std::vector<u8>> deserialize_from_log(const std::vector<u8>& wal_data) {
      if (wal_data.empty()) {
         throw std::invalid_argument("WAL data is empty.");
      }

      WALRecordType type = static_cast<WALRecordType>(wal_data[0]);
      std::vector<u8> data(wal_data.begin() + 1, wal_data.end());
      return {type, data};
   }

private:
    std::string logDir;
    uint64_t lastSequenceNo;
    uint32_t currentSegmentIndex;
    uint32_t oldestSegmentIndex;
    uint64_t byteOffset;
    uint32_t bufferSize;
    uint64_t maxSegmentSize;
    uint32_t maxSegmentCount;
    std::string walMode;
    std::string writeMode;
    bool stop;
    RetentionMode retentionMode;
    RotationMode rotationMode;
    std::map<uint64_t, uint64_t> namespaceLastSeqNo;  // Track last sequence number per namespace

    std::ofstream currentSegmentFile;
    std::thread syncThread;
    std::thread rotationThread;
    std::thread retentionThread;
    std::mutex mutex;

    uint32_t CalculateCRC32(const std::vector<uint8_t>& data);
    int GetEntrySize(const std::vector<uint8_t>& data);
    bool WriteEntryToBuffer(const WALEntry& entry);
    bool RotateLog();
    void DeleteOldestSegment();
    void Sync();

    void keepSyncingBuffer();
    void rotateSegmentPeriodically();
    void deleteSegmentPeriodically();

    // Recovery helper methods
    std::vector<fs::path> GetSegmentFiles() const;
    bool ReadWALEntry(std::ifstream& file, WALEntry& entry);
    bool ValidateWALEntry(const WALEntry& entry);
    void ParseAndReplayEntry(const WALEntry& entry, const ReplayCallback& callback);
    WALRecordType DetermineRecordType(const std::vector<uint8_t>& data);
};

#endif // AOF_HPP