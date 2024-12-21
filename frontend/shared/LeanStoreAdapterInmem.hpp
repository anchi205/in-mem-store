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
   leanstore::KVInterface* btree;
   string name;
   LeanStoreAdapter()
   {
      // hack
   }
   LeanStoreAdapter(LeanStore& db, string name) : name(name)
   {
      if (FLAGS_recover) {
         btree = &db.retrieveInmem(name);
      } else {
         btree = &db.registerInmem(name, {.enable_wal = FLAGS_wal, .use_bulk_insert = false});
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
};
