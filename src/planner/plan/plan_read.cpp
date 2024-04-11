#include "binder/expression_visitor.h"
#include "binder/query/reading_clause/bound_algorithm_call.h"
#include "binder/query/reading_clause/bound_in_query_call.h"
#include "binder/query/reading_clause/bound_load_from.h"
#include "binder/query/reading_clause/bound_match_clause.h"
#include "common/enums/join_type.h"
#include "planner/planner.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace planner {

void Planner::planReadingClause(const BoundReadingClause* readingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& prevPlans) {
    auto readingClauseType = readingClause->getClauseType();
    switch (readingClauseType) {
    case ClauseType::MATCH: {
        planMatchClause(readingClause, prevPlans);
    } break;
    case ClauseType::UNWIND: {
        planUnwindClause(readingClause, prevPlans);
    } break;
    case ClauseType::IN_QUERY_CALL: {
        planInQueryCall(readingClause, prevPlans);
    } break;
    case ClauseType::ALGORITHM_CALL: {
        planAlgorithmCall(readingClause, prevPlans);
    } break;
    case ClauseType::LOAD_FROM: {
        planLoadFrom(readingClause, prevPlans);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void Planner::planMatchClause(const BoundReadingClause* boundReadingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    auto boundMatchClause =
        ku_dynamic_cast<const BoundReadingClause*, const BoundMatchClause*>(boundReadingClause);
    auto queryGraphCollection = boundMatchClause->getQueryGraphCollection();
    auto predicates = boundMatchClause->getConjunctivePredicates();
    switch (boundMatchClause->getMatchClauseType()) {
    case MatchClauseType::MATCH: {
        if (plans.size() == 1 && plans[0]->isEmpty()) {
            plans = enumerateQueryGraphCollection(*queryGraphCollection, predicates);
        } else {
            for (auto& plan : plans) {
                planRegularMatch(*queryGraphCollection, predicates, *plan);
            }
        }
    } break;
    case MatchClauseType::OPTIONAL_MATCH: {
        for (auto& plan : plans) {
            expression_vector corrExprs;
            if (!plan->isEmpty()) {
                corrExprs =
                    getCorrelatedExprs(*queryGraphCollection, predicates, plan->getSchema());
            }
            planOptionalMatch(*queryGraphCollection, predicates, corrExprs, *plan);
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void Planner::planUnwindClause(const BoundReadingClause* boundReadingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    for (auto& plan : plans) {
        if (plan->isEmpty()) { // UNWIND [1, 2, 3, 4] AS x RETURN x
            appendDummyScan(*plan);
        }
        appendUnwind(*boundReadingClause, *plan);
    }
}

static bool hasExternalDependency(const std::shared_ptr<Expression>& expression,
    const std::unordered_set<std::string>& variableNameSet) {
    auto collector = ExpressionCollector();
    for (auto& name : collector.getDependentVariableNames(expression)) {
        if (!variableNameSet.contains(name)) {
            return true;
        }
    }
    return false;
}

static void splitPredicates(const expression_vector& outputExprs,
    const expression_vector& predicates, expression_vector& predicatesToPull,
    expression_vector& predicatesToPush) {
    std::unordered_set<std::string> columnNameSet;
    for (auto& column : outputExprs) {
        columnNameSet.insert(column->getUniqueName());
    }
    for (auto& predicate : predicates) {
        if (hasExternalDependency(predicate, columnNameSet)) {
            predicatesToPull.push_back(predicate);
        } else {
            predicatesToPush.push_back(predicate);
        }
    }
}

void Planner::planInQueryCall(const BoundReadingClause* readingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    auto inQueryCall = readingClause->constPtrCast<BoundInQueryCall>();
    expression_vector predicatesToPull;
    expression_vector predicatesToPush;
    splitPredicates(inQueryCall->getOutExprs(), inQueryCall->getConjunctivePredicates(),
        predicatesToPull, predicatesToPush);
    // Empty join condition. Table function does not take input from previous scope. So there is no
    // join condition.
    expression_vector joinConditions;
    for (auto& plan : plans) {
        planReadOp(getCall(*readingClause), predicatesToPush, joinConditions, *plan);
        if (!predicatesToPull.empty()) {
            appendFilters(predicatesToPull, *plan);
        }
    }
}

void Planner::planAlgorithmCall(const BoundReadingClause* readingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    auto algoCall = readingClause->constPtrCast<BoundAlgorithmCall>();
    expression_vector predicatesToPull;
    expression_vector predicatesToPush;
    splitPredicates(algoCall->getOutExprs(), algoCall->getConjunctivePredicates(), predicatesToPull,
        predicatesToPush);
    expression_vector joinConditions;
    for (auto& e : algoCall->getNodeInputs()) {
        auto node = e->constPtrCast<NodeExpression>();
        joinConditions.push_back(node->getInternalID());
    }
    for (auto& plan : plans) {
        planReadOp(getAlgorithm(*readingClause), predicatesToPush, joinConditions, *plan);
        if (!predicatesToPull.empty()) {
            appendFilters(predicatesToPull, *plan);
        }
    }
}

void Planner::planLoadFrom(const BoundReadingClause* readingClause,
    std::vector<std::unique_ptr<LogicalPlan>>& plans) {
    auto loadFrom = readingClause->constPtrCast<BoundLoadFrom>();
    expression_vector predicatesToPull;
    expression_vector predicatesToPush;
    splitPredicates(loadFrom->getInfo()->columns, loadFrom->getConjunctivePredicates(),
        predicatesToPull, predicatesToPush);
    // Empty join condition. LOAD FROM does not take input from previous scope. So there is no
    // join condition.
    expression_vector joinConditions;
    for (auto& plan : plans) {
        auto op = getScanFile(loadFrom->getInfo());
        planReadOp(std::move(op), predicatesToPush, joinConditions, *plan);
        if (!predicatesToPull.empty()) {
            appendFilters(predicatesToPull, *plan);
        }
    }
}

void Planner::planReadOp(std::shared_ptr<LogicalOperator> op, const expression_vector& predicates,
    const expression_vector& joinConditions, LogicalPlan& plan) {
    op->computeFactorizedSchema();
    if (!plan.isEmpty()) {
        auto tmpPlan = LogicalPlan();
        tmpPlan.setLastOperator(std::move(op));
        if (!predicates.empty()) {
            appendFilters(predicates, tmpPlan);
        }
        if (joinConditions.empty()) {
            appendCrossProduct(AccumulateType::REGULAR, plan, tmpPlan, plan);
        } else {
            appendHashJoin(joinConditions, JoinType::INNER, plan, tmpPlan, plan);
        }
    } else {
        plan.setLastOperator(std::move(op));
        if (!predicates.empty()) {
            appendFilters(predicates, plan);
        }
    }
}

} // namespace planner
} // namespace kuzu
