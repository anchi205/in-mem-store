#include "Inmem.hpp"

#include "leanstore/concurrency-recovery/CRMG.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <signal.h>
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


void Inmem::logOperation(uint64_t namespace_id, WALRecordType type, const std::vector<u8>& data) {
   if (config.enable_wal && aof) {
      // Prepare WAL entry data with type
      std::vector<u8> wal_data = serialize_for_log(type, data);
      aof->LogCommand(namespace_id, wal_data);
   }
}

void Inmem::replayOperation(uint64_t namespace_id, WALRecordType type, const u8* key, u16 key_length, const u8* value, u16 value_length) {
    // Start transaction with proper mode and isolation level
    cr::Worker::my().startTX(
        TX_MODE::OLTP,
        TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION,
        false,  // not read-only
        9999  // pass the namespace_id
    );

    try {
        switch (type) {
            case WALRecordType::INSERT: {
                std::vector<u8> key_copy(key, key + key_length);
                std::vector<u8> value_copy(value, value + value_length);
                auto result = insert(key_copy.data(), key_length, value_copy.data(), value_length);
                if (result != OP_RESULT::OK) {
                    std::cout << "Insert failed during replay because ok" << std::endl;
                    cr::Worker::my().abortTX();
                    return;
                }
                break;
            }
            case WALRecordType::UPDATE: {
                auto it = store.find(KeyValue(key, key_length, nullptr, 0));
                if (it != store.end()) {
                    store.erase(it);
                    store.insert({KeyValue(key, key_length, value, value_length), nullptr});
                }
                break;
            }
            case WALRecordType::REMOVE: {
                store.erase(KeyValue(key, key_length, nullptr, 0));
                break;
            }
        }
        
        // Commit the transaction if successful
        cr::Worker::my().commitTX();
    } catch (const std::exception& e) {
        std::cout << "Exception during replay: " << e.what() << std::endl;
        cr::Worker::my().abortTX();
    }
}

bool Inmem::StartRecovery() {
    if (!config.enable_wal || !aof) {
        return true;
    }

    return aof->StartRecovery([this](uint64_t namespace_id, WALRecordType type, const u8* key, u16 key_length, const u8* value, u16 value_length) {
        replayOperation(namespace_id, type, key, key_length, value, value_length);
    });
}

bool Inmem::RecoverNamespace(uint64_t namespace_id) {
    if (!config.enable_wal || !aof) {
        return true;
    }

    return aof->ReplayNamespace(namespace_id, [this](uint64_t ns_id, WALRecordType type, const u8* key, u16 key_length, const u8* value, u16 value_length) {
        replayOperation(ns_id, type, key, key_length, value, value_length);
    });
}

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
      return OP_RESULT::OK;
   }
   jumpmuCatch() {}
   UNREACHABLE();
   return OP_RESULT::OTHER;
}

// -------------------------------------------------------------------------------------

OP_RESULT Inmem::insert(u8* key, u16 key_length, u8* value, u16 value_length)
{
   cr::activeTX().markAsWrite();
   if (config.enable_wal) {
      cr::Worker::my().logging.walEnsureEnoughSpace(PAGE_SIZE * 1);
      auto& active_tx_ns = cr::activeTX().getNamespace();
      uint64_t namespace_id = active_tx_ns;
      // Log the insert operation with both key and value

      if(namespace_id != 9999) {
         std::vector<u8> log_data = serialize_for_wal(key, key_length, value, value_length);
         logOperation(namespace_id, WALRecordType::INSERT, log_data);
      }
      
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
   try
   {
      auto it = store.find(KeyValue(key, key_length, nullptr, 0));
      if (it != store.end()) {
         // Create a copy of the value, update it, then replace
         std::vector<u8> new_value = it->first.value;
         callback(new_value.data(), new_value.size());
         
         auto& active_tx_ns = cr::activeTX().getNamespace();
         uint64_t namespace_id = active_tx_ns;

         if (config.enable_wal) {
            // Log the update operation
            std::vector<u8> log_data = serialize_for_log(WALRecordType::UPDATE, std::vector<u8>(key, key + key_length));
            log_data.insert(log_data.end(), new_value.begin(), new_value.end());
            logOperation(namespace_id, WALRecordType::UPDATE, log_data);
         }
         
         // Remove old and insert new
         store.erase(it);
         store.insert({KeyValue(key, key_length, new_value.data(), new_value.size()), nullptr});
         return OP_RESULT::OK;
      }
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
         auto& active_tx_ns = cr::activeTX().getNamespace();
         uint64_t namespace_id = active_tx_ns;

         if (config.enable_wal) {
            // Log the remove operation
            std::vector<u8> log_data = serialize_for_log(WALRecordType::REMOVE, std::vector<u8>(key, key + key_length));
            logOperation(namespace_id, WALRecordType::REMOVE, log_data);
         }
         
         store.erase(it);
         return OP_RESULT::OK;
      }
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
   return store.size();
}

u64 Inmem::getHeight() {
   return 0; // Or implement actual height calculation logic
}

// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore

