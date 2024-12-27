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

struct WALEntry {
    uint8_t Version;
    uint64_t SequenceNo;
    std::vector<uint8_t> Data;
    uint32_t CRC32;
    uint64_t Timestamp;
};

class AOF {
public:
    AOF(const std::string& directory);
    ~AOF();

    bool Init();
    bool LogCommand(const std::vector<uint8_t>& data);

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
    RetentionMode retentionMode;
    RotationMode rotationMode;

    std::ofstream currentSegmentFile;
    std::thread syncThread;
    std::thread rotationThread;
    std::thread retentionThread;
    std::mutex mutex;
    bool stop;

    uint32_t CalculateCRC32(const std::vector<uint8_t>& data);
    int GetEntrySize(const std::vector<uint8_t>& data);
    bool WriteEntryToBuffer(const WALEntry& entry);
    bool RotateLog();
    void DeleteOldestSegment();
    void Sync();

    void keepSyncingBuffer();
    void rotateSegmentPeriodically();
    void deleteSegmentPeriodically();
};

#endif // AOF_HPP
