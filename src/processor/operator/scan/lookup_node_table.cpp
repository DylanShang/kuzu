#include "processor/operator/scan/lookup_node_table.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void OffsetScanNodeTable::init(common::nodeID_t nodeID) {
    nodeIDVector->setValue<nodeID_t>(0, nodeID);
    executed = false;
}

void OffsetScanNodeTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    for (auto& [_, nodeInfo] : tableIDToNodeInfo) {
        nodeInfo.localScanState = std::make_unique<NodeTableScanState>(nodeInfo.columnIDs);
        initVectors(*nodeInfo.localScanState, *resultSet);
    }
}

bool OffsetScanNodeTable::getNextTuplesInternal(ExecutionContext* context) {
    if (executed) {
        return false;
    }
    executed = true;
    auto transaction = context->clientContext->getTx();
    auto nodeID = nodeIDVector->getValue<nodeID_t>(0);
    KU_ASSERT(tableIDToNodeInfo.contains(nodeID.tableID));
    auto& nodeInfo = tableIDToNodeInfo.at(nodeID.tableID);
    // TODO(Guodong): The following lines are probably incorrect.
    nodeInfo.localScanState->source = TableScanSource::COMMITTED;
    nodeInfo.localScanState->nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeID.offset);
    nodeInfo.table->initializeScanState(transaction, *nodeInfo.localScanState);
    nodeInfo.table->lookup(transaction, *nodeInfo.localScanState);
    return true;
}

common::idx_t PrimaryKeyScanSharedState::getTableIdx() {
    std::unique_lock lck{mtx};
    if (cursor < numTables) {
        return cursor++;
    }
    return numTables;
}

void PrimaryKeyScanNodeTable::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanTable::initLocalStateInternal(resultSet, context);
    for (auto& nodeInfo : nodeInfos) {
        nodeInfo.localScanState = std::make_unique<NodeTableScanState>(nodeInfo.columnIDs);
        initVectors(*nodeInfo.localScanState, *resultSet);
    }
    indexEvaluator->init(*resultSet, context->clientContext->getMemoryManager());
}

bool PrimaryKeyScanNodeTable::getNextTuplesInternal(ExecutionContext* context) {
    auto transaction = context->clientContext->getTx();
    auto tableIdx = sharedState->getTableIdx();
    if (tableIdx >=  nodeInfos.size()) {
        return false;
    }
    KU_ASSERT(tableIdx < nodeInfos.size());
    auto& nodeInfo = nodeInfos[tableIdx];

    indexEvaluator->evaluate(context->clientContext);
    auto indexVector = indexEvaluator->resultVector.get();
    auto& selVector = indexVector->state->getSelVector();
    KU_ASSERT(selVector.getSelSize() == 1 && selVector.getSelectedPositions()[0] == 0);
    if (indexVector->isNull(0)) {
        return false;
    }

    offset_t nodeOffset;
    bool lookupSucceed = nodeInfo.table->getPKIndex()->lookup(transaction, indexVector, 0, nodeOffset);
    if (!lookupSucceed) {
        return false;
    }
    auto nodeID = nodeID_t {nodeOffset, nodeInfo.table->getTableID()};
    nodeInfo.localScanState->nodeIDVector->setValue<nodeID_t>(0, nodeID);
    nodeInfo.localScanState->source = TableScanSource::COMMITTED;
    nodeInfo.localScanState->nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    nodeInfo.table->initializeScanState(transaction, *nodeInfo.localScanState);
    nodeInfo.table->lookup(transaction, *nodeInfo.localScanState);
    return true;
}

} // namespace processor
} // namespace kuzu
