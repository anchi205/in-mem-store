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

bool AOF::LogCommand(const std::vector<uint8_t>& data) {
    std::lock_guard<std::mutex> lock(mutex);

    lastSequenceNo++;
    WALEntry entry{
        1, lastSequenceNo, data,
        CalculateCRC32(data),
        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count())
    };

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
