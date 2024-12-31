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
    uint64_t GetLastSequenceNumber() const { return lastSequenceNo; }
    std::vector<uint64_t> GetNamespaces() const;
    bool ReplayNamespace(uint64_t namespace_id, const ReplayCallback& callback);

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