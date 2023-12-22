#pragma once

#include <string>

#include "common/types/internal_id_t.h"

namespace kuzu {
namespace catalog {
class NodeTableSchema;
class RelTableSchema;
} // namespace catalog

namespace common {
class VirtualFileSystem;
} // namespace common

namespace storage {

class WALReplayerUtils {
public:
    static void removeHashIndexFile(
        common::VirtualFileSystem* vfs, common::table_id_t tableID, const std::string& directory);

    // Create empty hash index file for the new node table.
    static void createEmptyHashIndexFiles(catalog::NodeTableSchema* nodeTableSchema,
        const std::string& directory, common::VirtualFileSystem* vfs);
};

} // namespace storage
} // namespace kuzu
