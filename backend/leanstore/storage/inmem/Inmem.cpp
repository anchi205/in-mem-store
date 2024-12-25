#include "Inmem.hpp"

#include "leanstore/concurrency-recovery/CRMG.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <signal.h>
#include <fstream>
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace inmem
{
// -------------------------------------------------------------------------------------

// Write namespace to file in parent directory
void writeNamespaceToFile(const std::string& ns_str) {
   std::string filename = "/home/ayush/Documents/in-mem-store/namespace_log.txt";
   std::ofstream outfile;
   outfile.open(filename, std::ios::app); // Append mode
   if (outfile.is_open()) {
      outfile << ns_str << std::endl;
      outfile.close();
   }
}
// ns-wal-do
// maybe check here as a second priority to cpature the namespace_id from active transaction 
// and log lsn,namespace,function call,crc


OP_RESULT Inmem::lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback)
{
   while (true) {
      try
      {
         std::vector<u8> search_key(key, key + key_length);
         
         auto it = store.lower_bound(KeyValue(key, key_length, nullptr, 0));
         if (it != store.end() && std::equal(it->first.key.begin(), it->first.key.end(), search_key.begin(), search_key.end())) {
            payload_callback(it->first.value.data(), it->first.value.size());
            return OP_RESULT::OK;
         }
         auto& active_tx_ns = cr::activeTX().getNamespace();
         std::string namespace_id = to_string(active_tx_ns);
         writeNamespaceToFile(namespace_id);
         return OP_RESULT::NOT_FOUND;
      }
      catch(...)
      {
         WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
      }
   }
   UNREACHABLE();
   return OP_RESULT::OTHER;
}

// -------------------------------------------------------------------------------------

OP_RESULT Inmem::scanAsc(u8* start_key,
                           u16 key_length,
                           std::function<bool(const u8* key, u16 key_length, const u8* payload, u16 payload_length)> callback,
                           function<void()>)
{
   COUNTERS_BLOCK()
   {
      WorkerCounters::myCounters().dt_scan_asc[dt_id]++;
   }
   jumpmuTry()
   {
      auto it = store.lower_bound(KeyValue(start_key, key_length, nullptr, 0));
      while (it != store.end()) {
         if (!callback(it->first.key.data(), it->first.key.size(),
                     it->first.value.data(), it->first.value.size())) {
            break;
         }
         ++it;
      }
      auto& active_tx_ns = cr::activeTX().getNamespace();
      std::string namespace_id = to_string(active_tx_ns);
      writeNamespaceToFile(namespace_id);
      return OP_RESULT::OK;
   }
   jumpmuCatch() {}
   UNREACHABLE();
   return OP_RESULT::OTHER;
}

// -------------------------------------------------------------------------------------

OP_RESULT Inmem::scanDesc(u8* start_key, u16 key_length, std::function<bool(const u8*, u16, const u8*, u16)> callback, function<void()>)
{
   COUNTERS_BLOCK()
   {
      WorkerCounters::myCounters().dt_scan_desc[dt_id]++;
   }
   const Slice key(start_key, key_length);
   jumpmuTry()
   {
      auto it = store.lower_bound(KeyValue(start_key, key_length, nullptr, 0));
      while (it != store.begin()) {
         --it;
         if (!callback(it->first.key.data(), it->first.key.size(),
                     it->first.value.data(), it->first.value.size())) {
            break;
         }
      }
      auto& active_tx_ns = cr::activeTX().getNamespace();
      std::string namespace_id = to_string(active_tx_ns);
      writeNamespaceToFile(namespace_id);
      return OP_RESULT::OK;
   }
   jumpmuCatch() {}
   UNREACHABLE();
   return OP_RESULT::OTHER;
}

// -------------------------------------------------------------------------------------



// maybe come back
OP_RESULT Inmem::insert(u8* key, u16 key_length, u8* value, u16 value_length)
{
   cr::activeTX().markAsWrite();
   if (config.enable_wal) {
      cr::Worker::my().logging.walEnsureEnoughSpace(PAGE_SIZE * 1);
   }
   
   try
   {
      std::vector<u8> key_vec(key, key + key_length);

      // Check if the key already exists
      auto it = store.find(KeyValue(key, key_length, nullptr, 0));
      if (it != store.end()) {
         // Move the key to the front of the LRU list
         std::string key_str(key_vec.begin(), key_vec.end());
         auto lru_it = lru_map.find(key_str);
         if (lru_it != lru_map.end()) {
            lru_list.erase(lru_it->second);
         }
         lru_list.push_front(key_vec);
         lru_map[key_str] = lru_list.begin();
         return OP_RESULT::DUPLICATE;
      }

      // Check if the store is full
      if (store.size() >= MAX_STORE_SIZE) {
         // Evict the least recently used item
         auto lru_key = lru_list.back();
         std::string lru_key_str(lru_key.begin(), lru_key.end());
         store.erase(KeyValue(lru_key.data(), lru_key.size(), nullptr, 0));
         lru_map.erase(lru_key_str);
         lru_list.pop_back();
      }

      // Insert the new key-value pair
      store.insert({KeyValue(key, key_length, value, value_length), nullptr});
      std::string key_str(key_vec.begin(), key_vec.end());
      lru_list.push_front(key_vec);
      lru_map[key_str] = lru_list.begin();



      // ns-wal-do

      auto& active_tx_ns = cr::activeTX().getNamespace();
      std::string namespace_id = to_string(active_tx_ns);
      writeNamespaceToFile(namespace_id);

      return OP_RESULT::OK;
   }
   catch(...) {
      
   }
   UNREACHABLE();
   return OP_RESULT::OTHER;
}

// -------------------------------------------------------------------------------------


OP_RESULT Inmem::updateSameSizeInPlace(u8* key,
                                         u16 key_length,
                                         function<void(u8* payload, u16 payload_size)> callback,
                                         UpdateSameSizeInPlaceDescriptor&)
{
   cr::activeTX().markAsWrite();
   if (config.enable_wal) {
      cr::Worker::my().logging.walEnsureEnoughSpace(PAGE_SIZE * 1);
   }
   // Slice key(o_key, o_key_length);
   try
   {
      auto it = store.find(KeyValue(key, key_length, nullptr, 0));
      if (it != store.end()) {
         // Create a copy of the value, update it, then replace
         std::vector<u8> new_value = it->first.value;
         callback(new_value.data(), new_value.size());
         
         // Remove old and insert new
         store.erase(it);
         store.insert({KeyValue(key, key_length, new_value.data(), new_value.size()), nullptr});
         auto& active_tx_ns = cr::activeTX().getNamespace();
         std::string namespace_id = to_string(active_tx_ns);
         writeNamespaceToFile(namespace_id);
         return OP_RESULT::OK;
      }
      auto& active_tx_ns = cr::activeTX().getNamespace();
      std::string namespace_id = to_string(active_tx_ns);
      writeNamespaceToFile(namespace_id);
      return OP_RESULT::NOT_FOUND;
   }
   catch(...) {}
   UNREACHABLE();
   return OP_RESULT::NOT_FOUND;
}

// ----------------------------------------------------------------------------------

OP_RESULT Inmem::remove(u8* key, u16 key_length)
{
   cr::activeTX().markAsWrite();
   if (config.enable_wal) {
      cr::Worker::my().logging.walEnsureEnoughSpace(PAGE_SIZE * 1);
   }
   jumpmuTry()
   {
      auto it = store.find(KeyValue(key, key_length, nullptr, 0));
      if (it != store.end()) {
         store.erase(it);
         auto& active_tx_ns = cr::activeTX().getNamespace();
         std::string namespace_id = to_string(active_tx_ns);
         writeNamespaceToFile(namespace_id);
         return OP_RESULT::OK;
      }
      auto& active_tx_ns = cr::activeTX().getNamespace();
      std::string namespace_id = to_string(active_tx_ns);
      writeNamespaceToFile(namespace_id);
      return OP_RESULT::NOT_FOUND;
   }
   jumpmuCatch() {}
   UNREACHABLE();
   return OP_RESULT::OTHER;
}

// -------------------------------------------------------------------------------------

u64 Inmem::countPages() {
   return 1; // Or implement actual page counting logic
}

u64 Inmem::countEntries() {
   return 0; // Or implement actual entry counting logic
}

u64 Inmem::getHeight() {
   return 0; // Or implement actual height calculation logic
}

// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
