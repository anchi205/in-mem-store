#include "Inmem.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
#include <gflags/gflags.h>
#include <signal.h>
#include <algorithm>
#include <list>
#include <unordered_map>

using namespace std;
using namespace leanstore::storage;

namespace leanstore {
namespace storage {
namespace inmem {

const size_t MAX_STORE_SIZE = 1000; // Define a maximum size for the store

OP_RESULT Inmem::lookup(u8* key, u16 key_length, std::function<void(const u8*, u16)> payload_callback)
{
   std::vector<u8> search_key(key, key + key_length);
   std::lock_guard<std::mutex> lock(mutex);
   
   auto it = store.lower_bound(KeyValue(key, key_length, nullptr, 0));
   if (it != store.end() && std::equal(it->first.key.begin(), it->first.key.end(), search_key.begin(), search_key.end())) {
      payload_callback(it->first.value.data(), it->first.value.size());
      return OP_RESULT::OK;
   }
   return OP_RESULT::NOT_FOUND;
}

OP_RESULT Inmem::scanAsc(u8* start_key, u16 key_length, 
                        std::function<bool(const u8*, u16, const u8*, u16)> callback,
                        std::function<void()>)
{
   std::lock_guard<std::mutex> lock(mutex);
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

OP_RESULT Inmem::scanDesc(u8* start_key, u16 key_length,
                         std::function<bool(const u8*, u16, const u8*, u16)> callback,
                         std::function<void()>)
{
   std::lock_guard<std::mutex> lock(mutex);
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

OP_RESULT Inmem::insert(u8* key, u16 key_length, u8* value, u16 value_length)
{
   std::lock_guard<std::mutex> lock(mutex);
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

OP_RESULT Inmem::updateSameSizeInPlace(u8* key, u16 key_length,
                                     std::function<void(u8*, u16)> callback,
                                     UpdateSameSizeInPlaceDescriptor&)
{
   std::lock_guard<std::mutex> lock(mutex);
   auto it = store.find(KeyValue(key, key_length, nullptr, 0));
   if (it != store.end()) {
      // Create a copy of the value, update it, then replace
      std::vector<u8> new_value = it->first.value;
      callback(new_value.data(), new_value.size());
      
      // Remove old and insert new
      store.erase(it);
      store.insert({KeyValue(key, key_length, new_value.data(), new_value.size()), nullptr});
      return OP_RESULT::OK;
   }
   return OP_RESULT::NOT_FOUND;
}

OP_RESULT Inmem::remove(u8* key, u16 key_length)
{
   std::lock_guard<std::mutex> lock(mutex);
   auto it = store.find(KeyValue(key, key_length, nullptr, 0));
   if (it != store.end()) {
      store.erase(it);
      return OP_RESULT::OK;
   }
   return OP_RESULT::NOT_FOUND;
}

OP_RESULT Inmem::prefixLookup(u8* key, u16 key_length,
                            std::function<void(const u8*, u16, const u8*, u16)> payload_callback)
{
   std::lock_guard<std::mutex> lock(mutex);
   auto it = store.lower_bound(KeyValue(key, key_length, nullptr, 0));
   
   if (it != store.end() && 
       it->first.key.size() >= key_length &&
       std::equal(key, key + key_length, it->first.key.begin())) {
      payload_callback(it->first.key.data(), it->first.key.size(),
                      it->first.value.data(), it->first.value.size());
      return OP_RESULT::OK;
   }
   return OP_RESULT::NOT_FOUND;
}

OP_RESULT Inmem::prefixLookupForPrev(u8* key, u16 key_length,
                                   std::function<void(const u8*, u16, const u8*, u16)> payload_callback)
{
   std::lock_guard<std::mutex> lock(mutex);
   auto it = store.lower_bound(KeyValue(key, key_length, nullptr, 0));
   
   if (it != store.begin()) {
      --it;
      payload_callback(it->first.key.data(), it->first.key.size(),
                      it->first.value.data(), it->first.value.size());
      return OP_RESULT::OK;
   }
   return OP_RESULT::NOT_FOUND;
}

OP_RESULT Inmem::append(std::function<void(u8*)> o_key, u16 o_key_length,
                      std::function<void(u8*)> o_value, u16 o_value_length,
                      std::unique_ptr<u8[]>& session_ptr)
{
   std::vector<u8> key_buffer(o_key_length);
   std::vector<u8> value_buffer(o_value_length);
   
   o_key(key_buffer.data());
   o_value(value_buffer.data());
   
   std::lock_guard<std::mutex> lock(mutex);
   store.insert({KeyValue(key_buffer.data(), o_key_length, 
                         value_buffer.data(), o_value_length), nullptr});
   return OP_RESULT::OK;
}

OP_RESULT Inmem::rangeRemove(u8* start_key, u16 start_key_length,
                           u8* end_key, u16 end_key_length, bool)
{
   std::lock_guard<std::mutex> lock(mutex);
   auto start = store.lower_bound(KeyValue(start_key, start_key_length, nullptr, 0));
   auto end = store.upper_bound(KeyValue(end_key, end_key_length, nullptr, 0));
   
   store.erase(start, end);
   return OP_RESULT::OK;
}

u64 Inmem::countEntries()
{
   std::lock_guard<std::mutex> lock(mutex);
   return store.size();
}

u64 Inmem::countPages()
{
   return 1; // In-memory store doesn't have pages
}

u64 Inmem::getHeight()
{
   return 1; // In-memory store is flat
}

} // namespace inmem
} // namespace storage
} // namespace leanstore
