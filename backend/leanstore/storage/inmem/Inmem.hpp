#pragma once
#include "leanstore/Config.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include <map>
#include <vector>
#include <string>
#include <memory>
#include <list>
#include <unordered_map>
// -------------------------------------------------------------------------------------
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace storage {
namespace inmem {

class Inmem : public KVInterface {
private:
   struct KeyValue {
      std::vector<u8> key;
      std::vector<u8> value;
      
      KeyValue(const u8* k, u16 klen, const u8* v, u16 vlen) 
         : key(k, k + klen), value(v, v + vlen) {}
      
      bool operator<(const KeyValue& other) const {
         return std::lexicographical_compare(
            key.begin(), key.end(),
            other.key.begin(), other.key.end()
         );
      }
   };

   struct KeyCompare {
      bool operator()(const KeyValue& a, const KeyValue& b) const {
         return a < b;
      }
      
      bool operator()(const KeyValue& a, const std::vector<u8>& b) const {
         return std::lexicographical_compare(
            a.key.begin(), a.key.end(),
            b.begin(), b.end()
         );
      }
      
      bool operator()(const std::vector<u8>& a, const KeyValue& b) const {
         return std::lexicographical_compare(
            a.begin(), a.end(),
            b.key.begin(), b.key.end()
         );
      }
   };

   std::map<KeyValue, std::nullptr_t, KeyCompare> store;
   mutable std::mutex mutex;
   std::list<std::vector<u8>> lru_list;
   std::unordered_map<std::string, std::list<std::vector<u8>>::iterator> lru_map;
   const size_t MAX_STORE_SIZE = 1000000;  // Default max size

public:
   struct Config {
      bool enable_wal = true;
      bool use_bulk_insert = false;
   };

   DTID dt_id;
   Config config;

   Inmem() = default;
   // Interface implementations
   virtual OP_RESULT lookup(u8* key, u16 key_length, std::function<void(const u8*, u16)> payload_callback) override;
   virtual OP_RESULT insert(u8* key, u16 key_length, u8* value, u16 value_length) override;
   virtual OP_RESULT updateSameSizeInPlace(u8* key,
                                         u16 key_length,
                                         std::function<void(u8* value, u16 value_size)> callback,
                                         UpdateSameSizeInPlaceDescriptor& update_descriptor) override;
   virtual OP_RESULT remove(u8* key, u16 key_length) override;
   virtual OP_RESULT scanAsc(u8* start_key,
                           u16 key_length,
                           std::function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)> callback,
                           std::function<void()> undo) override;
   virtual OP_RESULT scanDesc(u8* start_key,
                            u16 key_length,
                            std::function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)> callback,
                            std::function<void()> undo) override;
   virtual OP_RESULT prefixLookup(u8* key, u16 key_length, std::function<void(const u8*, u16, const u8*, u16)> payload_callback) override;
   virtual OP_RESULT prefixLookupForPrev(u8* key, u16 key_length, std::function<void(const u8*, u16, const u8*, u16)> payload_callback) override;
   virtual OP_RESULT append(std::function<void(u8*)> o_key, u16 o_key_length,
                          std::function<void(u8*)> o_value, u16 o_value_length,
                          std::unique_ptr<u8[]>& session_ptr) override;
   virtual OP_RESULT rangeRemove(u8* start_key, u16 start_key_length,
                               u8* end_key, u16 end_key_length, bool page_wise) override;
   virtual u64 countPages() override;
   virtual u64 countEntries() override;
   virtual u64 getHeight() override;
   
   // Configuration management
   void create(DTID dtid, Config config);
   
   // Iterator support
   class Iterator {
   private:
      std::map<KeyValue, std::nullptr_t, KeyCompare>::iterator it;
      const Inmem* inmem;
      
   public:
      Iterator(Inmem* i) : inmem(i) {}
      bool isValid() const;
      void next();
      void prev(); 
      Slice key() const;
      Slice value() const;
   };
   
   // Statistics and metadata
   double averageSpaceUsage();
   u32 bytesFree();
   void printInfos(uint64_t totalSize);
   
   // Serialization support
   std::unordered_map<std::string, std::string> serialize();
   void deserialize(std::unordered_map<std::string, std::string> serialized);
   
   // Transaction support
   bool isVisibleForMe(WORKERID worker_id, TXID tx_ts, bool to_write = true);
};

} // namespace inmem
} // namespace storage
} // namespace leanstore
