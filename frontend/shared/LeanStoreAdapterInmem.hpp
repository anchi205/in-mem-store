#pragma once
#include "Adapter.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/LeanStore.hpp"
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <iostream>

using namespace leanstore;
template <class Record>
struct LeanStoreAdapterInmem : Adapter<Record> {
   leanstore::KVInterface* store;
   string name;
   LeanStoreAdapterInmem()
   {
      // hack
   }
   LeanStoreAdapterInmem(LeanStore& db, string name) : name(name)
   {
      // if (FLAGS_recover) {
      //    store = &db.retrieveInmem(name);
      // } else {
      //    store = &db.registerInmem(name, {.enable_wal = FLAGS_wal, .use_bulk_insert = false});
      // }

      store = &db.registerInmem(name, {.enable_wal = FLAGS_wal, .use_bulk_insert = false});

   }
   // -------------------------------------------------------------------------------------
  //  implemntation
  // - scanDesc
  // - insert
  // - lookup1
  // - update1
  // - erase
  // - lookupField
  // -  count

   void scan(const typename Record::Key& key,
            const std::function<bool(const typename Record::Key&, const Record&)>& callback,
            std::function<void()> reset = [](){}) override {
      u8 key_bytes[sizeof(typename Record::Key)];
      std::memcpy(key_bytes, &key, sizeof(typename Record::Key));
      
      store->scanAsc(key_bytes, sizeof(typename Record::Key),
         [&](const u8* key_ptr, u16 key_len, const u8* value_ptr, u16 value_len) {
            assert(key_len == sizeof(typename Record::Key));
            assert(value_len == sizeof(Record));
            return callback(*reinterpret_cast<const typename Record::Key*>(key_ptr),
                          *reinterpret_cast<const Record*>(value_ptr));
         },
         reset);
   }

   void scanDesc(const typename Record::Key& key,
                const std::function<bool(const typename Record::Key&, const Record&)>& callback,
                std::function<void()> reset = [](){}) override {
      u8 key_bytes[sizeof(typename Record::Key)];
      std::memcpy(key_bytes, &key, sizeof(typename Record::Key));
      
      store->scanDesc(key_bytes, sizeof(typename Record::Key),
         [&](const u8* key_ptr, u16 key_len, const u8* value_ptr, u16 value_len) {
            assert(key_len == sizeof(typename Record::Key));
            assert(value_len == sizeof(Record));
            return callback(*reinterpret_cast<const typename Record::Key*>(key_ptr),
                          *reinterpret_cast<const Record*>(value_ptr));
         },
         reset);
   }

   void insert(const typename Record::Key& key, const Record& record) override {
      u8 key_bytes[sizeof(typename Record::Key)];
      std::memcpy(key_bytes, &key, sizeof(typename Record::Key));
      
      auto result = store->insert(key_bytes, sizeof(typename Record::Key),
                               reinterpret_cast<u8*>(const_cast<Record*>(&record)),
                               sizeof(Record));
      if (result != OP_RESULT::OK) {
         throw std::runtime_error("Insert failed");
      }
   }

   void lookup1(const typename Record::Key& key, const std::function<void(const Record&)>& callback) override {
      u8 key_bytes[sizeof(typename Record::Key)];
      std::memcpy(key_bytes, &key, sizeof(typename Record::Key));
      
      store->lookup(key_bytes, sizeof(typename Record::Key),
         [&](const u8* payload_ptr, u16 payload_length) {
            assert(payload_length == sizeof(Record));
            callback(*reinterpret_cast<const Record*>(payload_ptr));
         });
   }

   void update1(const typename Record::Key& key,
               const std::function<void(Record&)>& callback,
               UpdateSameSizeInPlaceDescriptor& descriptor) override {
      u8 key_bytes[sizeof(typename Record::Key)];
      std::memcpy(key_bytes, &key, sizeof(typename Record::Key));
      
      auto result = store->updateSameSizeInPlace(
         key_bytes, sizeof(typename Record::Key),
         [&](u8* value_ptr, u16 value_size) {
            assert(value_size == sizeof(Record));
            callback(*reinterpret_cast<Record*>(value_ptr));
         },
         descriptor);
      if (result != OP_RESULT::OK) {
         throw std::runtime_error("Update failed");
      }
   }

   bool erase(const typename Record::Key& key) override {
      u8 key_bytes[sizeof(typename Record::Key)];
      std::memcpy(key_bytes, &key, sizeof(typename Record::Key));
      
      auto result = store->remove(key_bytes, sizeof(typename Record::Key));
      return result == OP_RESULT::OK;
   }

   template<typename Field>
   Field lookupField(const typename Record::Key& key, Field Record::*field) {
      Field result;
      lookup1(key, [&](const Record& record) {
         result = record.*field;
      });
      return result;
   }

   u64 count() {
      return store->countEntries();
   }
};
