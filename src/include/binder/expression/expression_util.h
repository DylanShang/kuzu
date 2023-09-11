#pragma once

#include "expression.h"

namespace kuzu {
namespace binder {

struct ExpressionUtil {
    static bool isExpressionsWithDataType(
        const expression_vector& expressions, common::LogicalTypeID dataTypeID);
    static expression_vector getExpressionsWithDataType(
        const expression_vector& expressions, common::LogicalTypeID dataTypeID);

    static uint32_t find(Expression* target, expression_vector expressions);

    // Print as a1,a2,a3,...
    static std::string toString(const expression_vector& expressions);
    // Print as a1=a2, a3=a4,...
    static std::string toString(const std::vector<expression_pair>& expressionPairs);
    // Print as a1=a2
    static std::string toString(const expression_pair& expressionPair);

    static expression_vector excludeExpressions(
        const expression_vector& expressions, const expression_vector& expressionsToExclude);

    inline static bool isNodeVariable(const Expression& expression) {
        return expression.expressionType == common::ExpressionType::VARIABLE &&
               expression.dataType.getLogicalTypeID() == common::LogicalTypeID::NODE;
    }
    inline static bool isRelVariable(const Expression& expression) {
        return expression.expressionType == common::ExpressionType::VARIABLE &&
               expression.dataType.getLogicalTypeID() == common::LogicalTypeID::REL;
    }
    inline static bool isRecursiveRelVariable(const Expression& expression) {
        return expression.expressionType == common::ExpressionType::VARIABLE &&
               expression.dataType.getLogicalTypeID() == common::LogicalTypeID::RECURSIVE_REL;
    }

    static std::vector<std::unique_ptr<common::LogicalType>> getDataTypes(
        const expression_vector& expressions);

    static expression_vector removeDuplication(const expression_vector& expressions);
};

} // namespace binder
} // namespace kuzu