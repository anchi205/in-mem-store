// aof.cpp
#include "aof.hpp"

AOF::AOF(const std::string& directory)
    : logDir(directory), lastSequenceNo(0), currentSegmentIndex(0),
      oldestSegmentIndex(0), byteOffset(0), bufferSize(4096),
      maxSegmentSize(10 * 1024 * 1024), maxSegmentCount(5),
      walMode("buffered"), writeMode("fsync"), retentionMode(RETENTION_MODE_TIME),
      rotationMode(ROTATION_MODE_TIME), stop(false) {
    syncThread = std::thread(&AOF::keepSyncingBuffer, this);
    rotationThread = std::thread(&AOF::rotateSegmentPeriodically, this);
    retentionThread = std::thread(&AOF::deleteSegmentPeriodically, this);
}

AOF::~AOF() {
    stop = true;
    if (syncThread.joinable()) syncThread.join();
    if (rotationThread.joinable()) rotationThread.join();
    if (retentionThread.joinable()) retentionThread.join();

    if (currentSegmentFile.is_open()) {
        Sync();
        currentSegmentFile.close();
    }
}

bool AOF::Init() {
    try {
        if (!fs::exists(logDir)) {
            fs::create_directories(logDir);
        }

        auto files = fs::directory_iterator(logDir);
        for (const auto& file : files) {
            if (file.path().filename().string().find(SEGMENT_PREFIX) != std::string::npos) {
                std::cout << "Found existing log segments: " << file.path() << std::endl;
            }
        }

        lastSequenceNo = 0;
        currentSegmentIndex = 0;
        oldestSegmentIndex = 0;
        byteOffset = 0;

        currentSegmentFile.open(fs::path(logDir) / (SEGMENT_PREFIX + "0"),
                                std::ios::out | std::ios::app | std::ios::binary);
        if (!currentSegmentFile.is_open()) {
            throw std::runtime_error("Failed to open initial segment file");
        }
    } catch (const std::exception& e) {
        std::cerr << "Error during initialization: " << e.what() << std::endl;
        return false;
    }

    return true;
}

bool AOF::LogCommand(uint64_t namespace_id, const std::vector<uint8_t>& data) {
    std::lock_guard<std::mutex> lock(mutex);

    lastSequenceNo++;
    namespaceLastSeqNo[namespace_id] = lastSequenceNo;  // Track last sequence number per namespace
    
    WALEntry entry;
    entry.Version = 1;
    entry.SequenceNo = lastSequenceNo;
    entry.NamespaceId = namespace_id;
    entry.Data = data;
    entry.CRC32 = CalculateCRC32(data);
    entry.Timestamp = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());

    int entrySize = GetEntrySize(data);
    if (byteOffset + entrySize > maxSegmentSize) {
        if (!RotateLog()) {
            return false;
        }
    }

    byteOffset += entrySize;

    return WriteEntryToBuffer(entry);
}

uint32_t AOF::CalculateCRC32(const std::vector<uint8_t>& data) {
    return crc32(0, reinterpret_cast<const uint8_t*>(data.data()), data.size());
}

int AOF::GetEntrySize(const std::vector<uint8_t>& data) {
    return sizeof(WALEntry) + data.size();
}

bool AOF::WriteEntryToBuffer(const WALEntry& entry) {
    try {
        std::ostringstream oss;
        oss.write(reinterpret_cast<const char*>(&entry), sizeof(entry));
        oss.write(reinterpret_cast<const char*>(entry.Data.data()), entry.Data.size());

        currentSegmentFile.write(oss.str().c_str(), oss.str().size());
        if (walMode == "unbuffered") {
            Sync();
        }
    } catch (const std::exception& e) {
        std::cerr << "Error writing entry to buffer: " << e.what() << std::endl;
        return false;
    }

    return true;
}

bool AOF::RotateLog() {
    Sync();

    currentSegmentFile.close();
    currentSegmentIndex++;

    if (currentSegmentIndex - oldestSegmentIndex + 1 > maxSegmentCount) {
        DeleteOldestSegment();
        oldestSegmentIndex++;
    }

    currentSegmentFile.open(fs::path(logDir) / (SEGMENT_PREFIX + std::to_string(currentSegmentIndex)),
                            std::ios::out | std::ios::app | std::ios::binary);

    if (!currentSegmentFile.is_open()) {
        std::cerr << "Failed to open new segment file" << std::endl;
        return false;
    }

    byteOffset = 0;
    return true;
}

void AOF::DeleteOldestSegment() {
    fs::remove(fs::path(logDir) / (SEGMENT_PREFIX + std::to_string(oldestSegmentIndex)));
}

void AOF::Sync() {
    if (currentSegmentFile.is_open()) {
        currentSegmentFile.flush();
    }
}

void AOF::keepSyncingBuffer() {
    while (!stop) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        Sync();
    }
}

void AOF::rotateSegmentPeriodically() {
    while (!stop) {
        std::this_thread::sleep_for(std::chrono::seconds(60));
        RotateLog();
    }
}

void AOF::deleteSegmentPeriodically() {
    while (!stop) {
        std::this_thread::sleep_for(std::chrono::hours(1));
        DeleteOldestSegment();
    }
}

// Recovery Methods Implementation
bool AOF::StartRecovery(const ReplayCallback& callback) {
    auto segments = GetSegmentFiles();
    if (segments.empty()) {
        return true;  // No segments to recover from
    }

    const size_t BATCH_SIZE = 1024 * 1024;  // Process 1MB at a time
    std::vector<char> buffer(BATCH_SIZE);

    for (const auto& segment : segments) {
        std::ifstream file(segment, std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "Failed to open segment file: " << segment << std::endl;
            return false;
        }

        while (!file.eof()) {
            WALEntry entry;
            if (!ReadWALEntry(file, entry)) {
                if (file.eof()) break;
                std::cerr << "Failed to read WAL entry in " << segment << std::endl;
                continue;
            }

            if (!ValidateWALEntry(entry)) {
                std::cerr << "Invalid WAL entry found in " << segment << std::endl;
                continue;
            }

            ParseAndReplayEntry(entry, callback);
            lastSequenceNo = std::max(lastSequenceNo, entry.SequenceNo);
            namespaceLastSeqNo[entry.NamespaceId] = entry.SequenceNo;

            // Release memory after processing each entry
            entry.Data.clear();
            entry.Data.shrink_to_fit();
        }
        file.close();
    }

    return true;
}

std::vector<uint64_t> AOF::GetNamespaces() const {
    std::vector<uint64_t> namespaces;
    for (const auto& [namespace_id, _] : namespaceLastSeqNo) {
        namespaces.push_back(namespace_id);
    }
    return namespaces;
}

bool AOF::ReplayNamespace(uint64_t namespace_id, const ReplayCallback& callback) {
    auto segments = GetSegmentFiles();
    if (segments.empty()) {
        return true;  // No segments to recover from
    }

    for (const auto& segment : segments) {
        std::ifstream file(segment, std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "Failed to open segment file: " << segment << std::endl;
            return false;
        }

        WALEntry entry;
        while (ReadWALEntry(file, entry)) {
            if (!ValidateWALEntry(entry) || entry.NamespaceId != namespace_id) {
                continue;
            }

            ParseAndReplayEntry(entry, callback);
        }
    }

    return true;
}

std::vector<fs::path> AOF::GetSegmentFiles() const {
    std::vector<fs::path> segments;
    for (const auto& entry : fs::directory_iterator(logDir)) {
        if (entry.path().filename().string().find(SEGMENT_PREFIX) == 0) {
            segments.push_back(entry.path());
        }
    }
    std::sort(segments.begin(), segments.end());  // Sort by filename to ensure order
    return segments;
}

bool AOF::ReadWALEntry(std::ifstream& file, WALEntry& entry) {
    // Read the fixed-size portion of the entry
    if (!file.read(reinterpret_cast<char*>(&entry.Version), sizeof(entry.Version)) ||
        !file.read(reinterpret_cast<char*>(&entry.SequenceNo), sizeof(entry.SequenceNo)) ||
        !file.read(reinterpret_cast<char*>(&entry.NamespaceId), sizeof(entry.NamespaceId)) ||
        !file.read(reinterpret_cast<char*>(&entry.CRC32), sizeof(entry.CRC32)) ||
        !file.read(reinterpret_cast<char*>(&entry.Timestamp), sizeof(entry.Timestamp))) {
        return false;
    }

    // Read the variable-length data
    uint32_t dataSize;
    if (!file.read(reinterpret_cast<char*>(&dataSize), sizeof(dataSize))) {
        return false;
    }

    entry.Data.resize(dataSize);
    if (!file.read(reinterpret_cast<char*>(entry.Data.data()), dataSize)) {
        return false;
    }

    return true;
}

bool AOF::ValidateWALEntry(const WALEntry& entry) {
    // Verify CRC32
    uint32_t computed_crc = CalculateCRC32(entry.Data);
    if (computed_crc != entry.CRC32) {
        std::cerr << "CRC32 mismatch for entry " << entry.SequenceNo << std::endl;
        return false;
    }

    // Verify version
    if (entry.Version != 1) {
        std::cerr << "Unsupported WAL entry version: " << entry.Version << std::endl;
        return false;
    }

    return true;
}

void AOF::ParseAndReplayEntry(const WALEntry& entry, const ReplayCallback& callback) {
    const std::vector<uint8_t>& data = entry.Data;
    WALRecordType type = DetermineRecordType(data);
    
    // First 2 bytes are key length
    u16 key_length = *reinterpret_cast<const u16*>(data.data());
    const u8* key_ptr = data.data() + sizeof(u16);
    
    // Next 2 bytes after key are value length
    u16 value_length = *reinterpret_cast<const u16*>(key_ptr + key_length);
    const u8* value_ptr = key_ptr + key_length + sizeof(u16);

    callback(entry.NamespaceId, type, key_ptr, key_length, value_ptr, value_length);
}

WALRecordType AOF::DetermineRecordType(const std::vector<uint8_t>& data) {
    // The record type is stored in the first byte of the data
    return static_cast<WALRecordType>(data[0]);
}