#pragma once
#include <string>

namespace leanstore {
namespace storage {
namespace inmem {

class MemWALManager {
public:
    MemWALManager() = default;
    void writeMemWALEntry(const std::string& namespace_id);
};

}  // namespace inmem
}  // namespace storage
}  // namespace leanstore 