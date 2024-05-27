#pragma once

#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "storage/store/table_data.h"

namespace kuzu {
namespace storage {
class TablesStatistics;

class LocalNodeNG;
struct NodeDataScanState final : TableDataScanState {
    explicit NodeDataScanState(const std::vector<common::column_id_t>& columnIDs)
        : TableDataScanState{columnIDs} {}
    DELETE_COPY_DEFAULT_MOVE(NodeDataScanState);

    common::row_idx_t numRowsToScan = 0;
    common::vector_idx_t vectorIdx = common::INVALID_VECTOR_IDX;
    common::row_idx_t numRowsInNodeGroup = 0;

    bool nextVector();
};

class LocalTableData;
class NodeTableData final : public TableData {
public:
    NodeTableData(BMFileHandle* dataFH, BMFileHandle* metadataFH,
        const catalog::TableCatalogEntry* tableEntry, BufferManager* bufferManager, WAL* wal,
        const std::vector<catalog::Property>& properties, TablesStatistics* tablesStatistics,
        bool enableCompression);

    // This interface is node table specific, as rel table requires also relDataDirection.
    void initializeScanState(transaction::Transaction* transaction,
        TableScanState& scanState) const override;
    void scan(transaction::Transaction* transaction, TableScanState& scanState,
        common::ValueVector& nodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) override;

    // Flush the nodeGroup to disk and update metadataDAs.
    void append(transaction::Transaction* transaction, ChunkedNodeGroup* nodeGroup) override;

    void prepareLocalNodeGroupToCommit(common::node_group_idx_t nodeGroupIdx,
        transaction::Transaction* transaction, LocalNodeNG* localNodeGroup) const;
    void prepareLocalTableToCommit(transaction::Transaction* transaction,
        LocalTableData* localTable) override;

    common::node_group_idx_t getNumCommittedNodeGroups() const override {
        return columns[0]->getNumCommittedNodeGroups();
    }

    common::node_group_idx_t getNumNodeGroups(const transaction::Transaction* transaction) const {
        // TODO(Guodong): FIX-ME. Take transaction into consideration.
        return columns[0]->getNumNodeGroups(transaction);
    }
    common::offset_t getNumTuplesInNodeGroup(const transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx) const;

private:
    void loadNodeGroups(const std::vector<catalog::Property>& properties);

    void initializeColumnScanStates(transaction::Transaction* transaction,
        TableScanState& scanState, common::node_group_idx_t nodeGroupIdx) const;
    void initializeLocalNodeReadState(transaction::Transaction* transaction,
        TableScanState& scanState, common::node_group_idx_t nodeGroupIdx) const;

    void lookup(transaction::Transaction* transaction, TableScanState& state,
        const common::ValueVector& nodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) override;

    static bool sanityCheckOnColumnNumValues(const NodeDataScanState& scanState);

private:
    std::mutex mtx;
    common::offset_t nextNodeOffset;
};

} // namespace storage
} // namespace kuzu
