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

using namespace leanstore;
template <class Record>
struct LeanStoreAdapter : Adapter<Record> {
   leanstore::KVInterface* store;
   string name;
   LeanStoreAdapter()
   {
      // hack
   }
   LeanStoreAdapter(LeanStore& db, string name) : name(name)
   {
      if (FLAGS_recover) {
         store = &db.retrieveInmem(name);
      } else {
         store = &db.registerInmem(name, {.enable_wal = FLAGS_wal, .use_bulk_insert = false});
      }

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

   void scanDesc(const typename Record::Key& key, const std::function<bool(const typename Record::Key&, const Record&)>& callback) {
      u8 key_bytes[sizeof(typename Record::Key)];
      std::memcpy(key_bytes, &key, sizeof(typename Record::Key));
      
      store->scanDesc(key_bytes, sizeof(typename Record::Key),
         [&](const u8* key_ptr, u16 key_len, const u8* value_ptr, u16 value_len) {
            assert(key_len == sizeof(typename Record::Key));
            assert(value_len == sizeof(Record));
            return callback(*reinterpret_cast<const typename Record::Key*>(key_ptr),
                          *reinterpret_cast<const Record*>(value_ptr));
         },
         []() {});
   }

   bool insert(const typename Record::Key& key, const Record& record) {
      u8 key_bytes[sizeof(typename Record::Key)];
      std::memcpy(key_bytes, &key, sizeof(typename Record::Key));
      
      auto result = store->insert(key_bytes, sizeof(typename Record::Key),
                                reinterpret_cast<u8*>(const_cast<Record*>(&record)),
                                sizeof(Record));
      return result == OP_RESULT::OK;
   }

   bool lookup1(const typename Record::Key& key, Record& record) {
      bool found = false;
      u8 key_bytes[sizeof(typename Record::Key)];
      std::memcpy(key_bytes, &key, sizeof(typename Record::Key));
      
      auto result = store->lookup(key_bytes, sizeof(typename Record::Key),
         [&](const u8* payload_ptr, u16 payload_length) {
            assert(payload_length == sizeof(Record));
            std::memcpy(&record, payload_ptr, sizeof(Record));
            found = true;
         });
      return found;
   }

   bool update1(const typename Record::Key& key, const Record& record) {
      u8 key_bytes[sizeof(typename Record::Key)];
      std::memcpy(key_bytes, &key, sizeof(typename Record::Key));
      
      UpdateSameSizeInPlaceDescriptor descriptor;
      auto result = store->updateSameSizeInPlace(
         key_bytes, sizeof(typename Record::Key),
         [&](u8* value_ptr, u16 value_size) {
            assert(value_size == sizeof(Record));
            std::memcpy(value_ptr, &record, sizeof(Record));
         },
         descriptor);
      return result == OP_RESULT::OK;
   }

   bool erase(const typename Record::Key& key) {
      u8 key_bytes[sizeof(typename Record::Key)];
      std::memcpy(key_bytes, &key, sizeof(typename Record::Key));
      
      auto result = store->remove(key_bytes, sizeof(typename Record::Key));
      return result == OP_RESULT::OK;
   }

   template<typename Field>
   bool lookupField(const typename Record::Key& key, Field Record::*field, Field& result) {
      Record record;
      if (lookup1(key, record)) {
         result = record.*field;
         return true;
      }
      return false;
   }

   u64 count() {
      return store->countEntries();
   }
};
