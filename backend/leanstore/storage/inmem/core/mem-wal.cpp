#include "mem-wal.hpp"
#include <chrono>
#include <iomanip>
#include <sstream>
#include <filesystem>
#include <iostream>

namespace leanstore {
namespace storage {
namespace inmem {

MemWALManager::MemWALManager() {
    // Initialize with custom config values
    config.segment_prefix = "seg-";
    config.wal_path = "/tmp/memwal";
    config.wal_buffer_size = 1024 * 1024;  // 1MB buffer
    config.wal_segment_size = 64 * 1024 * 1024;  // 64MB segments
    config.wal_buffer_sync_ms = 200;
    MemWALlsn = 0;

    // // Create WAL directory if it doesn't exist
    // std::filesystem::create_directories(config.wal_path);
    
    // // Open initial WAL file
    // std::string initial_wal_file = config.wal_path + "/" + config.segment_prefix + "1.wal";
    // wal_file.open(initial_wal_file, std::ios::app | std::ios::binary);
    
    // if (!wal_file.is_open()) {
    //     std::cerr << "Failed to open WAL file: " << initial_wal_file << std::endl;
    // }
}


void MemWALManager::writeMemWALEntry(const std::string& ns_str) {
  std::string filename = "/home/ayush/Documents/in-mem-store/namespace_log.txt";
  std::ofstream outfile;
  outfile.open(filename, std::ios::app); // Append mode
  if (outfile.is_open()) {
    outfile << ns_str << std::endl;
    outfile.close();
  }
}

// void MemWALManager::writeMemWALEntry() {
//     if (!wal_file.is_open()) {
//         return;
//     }

//     // Get current timestamp
//     auto now = std::chrono::system_clock::now();
//     auto now_time = std::chrono::system_clock::to_time_t(now);
    
//     // Create WAL entry
//     std::stringstream entry;
//     entry << "LSN:" << ++MemWALlsn << "|";
//     entry << "TIME:" << std::put_time(std::localtime(&now_time), "%Y-%m-%d %H:%M:%S") << "\n";
    
//     wal_file << entry.str();
//     wal_file.flush();

//     // Check if we need to rotate segment
//     if (wal_file.tellp() >= static_cast<std::streampos>(config.wal_segment_size)) {
//         rotateSegment();
//     }
// }

// void MemWALManager::recoverMemWALEntries() {
//     // Scan WAL directory for segment files
//     std::string wal_dir = config.wal_path;
//     std::string prefix = config.segment_prefix;
    
//     for (const auto& entry : std::filesystem::directory_iterator(wal_dir)) {
//         if (entry.path().filename().string().starts_with(prefix)) {
//             std::ifstream segment_file(entry.path(), std::ios::binary);
//             if (segment_file.is_open()) {
//                 std::string line;
//                 while (std::getline(segment_file, line)) {
//                     // Process each WAL entry
//                     // TODO: Parse and apply WAL entries
//                 }
//                 segment_file.close();
//             }
//         }
//     }
// }

// void MemWALManager::closeMemWAL() {
//     if (wal_file.is_open()) {
//         SyncBuffer();
//         wal_file.close();
//     }
// }

// void MemWALManager::changeWALFile() {
//     if (wal_file.is_open()) {
//         wal_file.close();
//     }

//     // Generate new WAL file name
//     std::string new_wal_file = config.wal_path + "/" + config.segment_prefix + 
//                               std::to_string(MemWALlsn) + ".wal";
    
//     wal_file.open(new_wal_file, std::ios::app | std::ios::binary);
// }

// void MemWALManager::rotateSegment() {
//     SyncBuffer();
//     changeWALFile();
//     deleteOldSegment();
// }

// void MemWALManager::deleteOldSegment() {
//     // Scan directory for old segments
//     std::string wal_dir = config.wal_path;
//     std::string prefix = config.segment_prefix;
    
//     std::vector<std::filesystem::path> segments;
//     for (const auto& entry : std::filesystem::directory_iterator(wal_dir)) {
//         if (entry.path().filename().string().starts_with(prefix)) {
//             segments.push_back(entry.path());
//         }
//     }

//     // Sort segments by name (which includes LSN)
//     std::sort(segments.begin(), segments.end());

//     // Keep only the last few segments, delete others
//     const size_t segments_to_keep = 5; // Configurable number of segments to retain
//     while (segments.size() > segments_to_keep) {
//         std::filesystem::remove(segments.front());
//         segments.erase(segments.begin());
//     }
// }

// void MemWALManager::SyncBuffer() {
//     if (wal_file.is_open()) {
//         wal_file.flush();
//         // Force sync to disk
//         int fd = fileno(wal_file.rdbuf()->stdio_file());
//         fsync(fd);
//     }
// }

} // namespace inmem
} // namespace storage
} // namespace leanstore
