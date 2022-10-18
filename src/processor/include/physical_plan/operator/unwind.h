#pragma once

#include "src/binder/expression/include/expression.h"
#include "src/common/include/vector/value_vector_utils.h"
#include "src/expression_evaluator/include/base_evaluator.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/source_operator.h"
#include "src/processor/include/physical_plan/result/result_set.h"

using namespace graphflow::evaluator;

namespace graphflow {
namespace processor {

class Unwind : public PhysicalOperator {
public:
    Unwind(unique_ptr<DataType> outDataType, DataPos outDataPos,
        unique_ptr<BaseExpressionEvaluator> expressionEvaluator, unique_ptr<PhysicalOperator> child,
        uint32_t id, const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString}, outDataType{move(outDataType)},
          outDataPos{outDataPos}, expressionEvaluator{move(expressionEvaluator)}, startIndex{0u} {}

    inline PhysicalOperatorType getOperatorType() override { return PhysicalOperatorType::UNWIND; }

    bool getNextTuples() override;

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Unwind>(move(outDataType), outDataPos, expressionEvaluator->clone(),
            children[0]->clone(), id, paramsString);
    }

private:
    bool hasMoreToRead() const;
    void copyTuplesToOutVector(uint64_t startPos, uint64_t endPos) const;

    unique_ptr<DataType> outDataType;
    DataPos outDataPos;

    unique_ptr<BaseExpressionEvaluator> expressionEvaluator;
    shared_ptr<ValueVector> outValueVector;
    uint32_t startIndex;
    gf_list_t inputList;
};

} // namespace processor
} // namespace graphflow