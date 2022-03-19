#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"

using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class BaseScanColumn : public PhysicalOperator {

public:
    BaseScanColumn(const DataPos& inputNodeIDVectorPos, unique_ptr<PhysicalOperator> child,
        ExecutionContext& context, uint32_t id)
        : PhysicalOperator{move(child), context, id}, inputNodeIDVectorPos{inputNodeIDVectorPos} {}

    PhysicalOperatorType getOperatorType() override = 0;

    shared_ptr<ResultSet> initResultSet() override;

    void reInitToRerunSubPlan() override;

    void printMetricsToJson(nlohmann::json& json, Profiler& profiler) override;

protected:
    DataPos inputNodeIDVectorPos;

    shared_ptr<DataChunk> inputNodeIDDataChunk;
    shared_ptr<ValueVector> inputNodeIDVector;
};

class ScanSingleColumn : public BaseScanColumn {

protected:
    ScanSingleColumn(const DataPos& inputNodeIDVectorPos, const DataPos& outputVectorPos,
        unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id)
        : BaseScanColumn{inputNodeIDVectorPos, move(child), context, id}, outputVectorPos{
                                                                              outputVectorPos} {}

protected:
    DataPos outputVectorPos;

    shared_ptr<ValueVector> outputVector;
};

class ScanMultipleColumns : public BaseScanColumn {

protected:
    ScanMultipleColumns(const DataPos& inputNodeIDVectorPos, vector<DataPos> outputVectorsPos,
        unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id)
        : BaseScanColumn{inputNodeIDVectorPos, move(child), context, id}, outputVectorsPos{move(
                                                                              outputVectorsPos)} {}

protected:
    vector<DataPos> outputVectorsPos;
    vector<shared_ptr<ValueVector>> outputVectors;
};

} // namespace processor
} // namespace graphflow