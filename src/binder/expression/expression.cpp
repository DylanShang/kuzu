#include "binder/expression/expression.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

expression_vector Expression::splitOnAND() {
    expression_vector result;
    if (AND == expressionType) {
        for (auto& child : children) {
            for (auto& exp : child->splitOnAND()) {
                result.push_back(exp);
            }
        }
    } else {
        result.push_back(shared_from_this());
    }
    return result;
}

bool ExpressionUtil::allExpressionsHaveDataType(
    expression_vector& expressions, LogicalTypeID dataTypeID) {
    for (auto& expression : expressions) {
        if (expression->dataType.getLogicalTypeID() != dataTypeID) {
            return false;
        }
    }
    return true;
}

uint32_t ExpressionUtil::find(Expression* target, expression_vector expressions) {
    for (auto i = 0u; i < expressions.size(); ++i) {
        if (target->getUniqueName() == expressions[i]->getUniqueName()) {
            return i;
        }
    }
    return UINT32_MAX;
}

std::string ExpressionUtil::toString(const expression_vector& expressions) {
    if (expressions.empty()) {
        return std::string{};
    }
    auto result = expressions[0]->toString();
    for (auto i = 1u; i < expressions.size(); ++i) {
        result += "," + expressions[i]->toString();
    }
    return result;
}

expression_vector ExpressionUtil::excludeExpressions(
    const expression_vector& expressions, const expression_vector& expressionsToExclude) {
    expression_set excludeSet;
    for (auto& expression : expressionsToExclude) {
        excludeSet.insert(expression);
    }
    expression_vector result;
    for (auto& expression : expressions) {
        if (!excludeSet.contains(expression)) {
            result.push_back(expression);
        }
    }
    return result;
}

} // namespace binder
} // namespace kuzu
