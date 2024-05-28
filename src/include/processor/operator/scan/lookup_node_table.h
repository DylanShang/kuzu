#pragma once

#include "processor/operator/scan/scan_node_table.h"
#include "expression_evaluator/expression_evaluator.h"

namespace kuzu {
namespace processor {

// OffsetScanNodeTable is only used as the source operator for RecursiveJoin and thus cannot be
// executed in parallel. Therefore, the shared state is equivalent to a local state. We bypass
// grabing lock and directly modify its field
class OffsetScanNodeTable : public ScanTable {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::OFFSET_SCAN_NODE_TABLE;

public:
    OffsetScanNodeTable(ScanTableInfo info, common::table_id_map_t<ScanNodeTableInfo> tableIDToNodeInfo,
        uint32_t id, const std::string& paramString)
        : ScanTable{type_, std::move(info), id, paramString},
          tableIDToNodeInfo{std::move(tableIDToNodeInfo)}, executed{false} {}

    void init(common::nodeID_t nodeID);

    bool isSource() const override { return true; }

    void initLocalStateInternal(ResultSet*, ExecutionContext*) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<OffsetScanNodeTable>(info.copy(), copyMap(tableIDToNodeInfo), id,
            paramsString);
    }

private:
    common::table_id_map_t<ScanNodeTableInfo> tableIDToNodeInfo;
    bool executed;
};

struct PrimaryKeyScanSharedState {
    std::mutex mtx;

    common::idx_t numTables;
    common::idx_t cursor;

    explicit PrimaryKeyScanSharedState(common::idx_t numTables)
        : numTables{numTables}, cursor{0} {}

    common::idx_t getTableIdx();
};

class PrimaryKeyScanNodeTable : public ScanTable {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::PRIMARY_KEY_SCAN_NODE_TABLE;

public:
    PrimaryKeyScanNodeTable(ScanTableInfo info, std::vector<ScanNodeTableInfo> nodeInfos,
        std::unique_ptr<evaluator::ExpressionEvaluator> indexEvaluator, std::shared_ptr<PrimaryKeyScanSharedState> sharedState, uint32_t id,
        const std::string& paramString)
        : ScanTable{type_, std::move(info), id, paramString}, nodeInfos{std::move(nodeInfos)},
          indexEvaluator{std::move(indexEvaluator)}, sharedState{std::move(sharedState)} {}


    bool isSource() const override { return true; }

    void initLocalStateInternal(ResultSet *, ExecutionContext *) override;

    bool getNextTuplesInternal(ExecutionContext *context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<PrimaryKeyScanNodeTable>(info.copy(), copyVector(nodeInfos), indexEvaluator->clone(), sharedState, id, paramsString);
    }

private:
    std::vector<ScanNodeTableInfo> nodeInfos;
    std::unique_ptr<evaluator::ExpressionEvaluator> indexEvaluator;
    std::shared_ptr<PrimaryKeyScanSharedState> sharedState;
};




} // namespace processor
} // namespace kuzu
