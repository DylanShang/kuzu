#include "include/enumerator.h"

#include "src/binder/query/include/bound_regular_query.h"
#include "src/planner/logical_plan/logical_operator/include/logical_intersect.h"

namespace graphflow {
namespace planner {

static const bool ENABLE_ASP = true;

static expression_vector extractPredicatesForNode(
    expression_vector& predicates, NodeExpression& node) {
    expression_vector result;
    for (auto& predicate : predicates) {
        auto names = predicate->getDependentVariableNames();
        if (names.size() == 1 && names.contains(node.getUniqueName())) {
            result.push_back(predicate);
        }
    }
    return result;
}

unique_ptr<LogicalPlan> Enumerator::getIS2Plan(const BoundStatement& statement) {
    auto queryPart = extractQueryPart(statement);
    auto queryGraph = queryPart->getQueryGraph(0);
    auto predicates = queryPart->getQueryGraphPredicate(0)->splitOnAND();
    auto p = queryGraph->getQueryNode(0);
    assert(p->getRawName() == "p");
    auto c = queryGraph->getQueryNode(1);
    assert(c->getRawName() == "c");
    auto post = queryGraph->getQueryNode(2);
    assert(post->getRawName() == "post");
    auto op = queryGraph->getQueryNode(3);
    assert(op->getRawName() == "op");
    auto e1 = queryGraph->getQueryRel(0);
    assert(e1->getRawName() == "e1");
    auto e2 = queryGraph->getQueryRel(1);
    assert(e2->getRawName() == "e2");
    auto e3 = queryGraph->getQueryRel(2);
    assert(e3->getRawName() == "e3");

    auto plan = createRelScanPlan(e1, p, predicates, true);
    compileHashJoinWithNode(*plan, c, predicates);
    auto scanE2Plan = createRelScanPlan(e2, c, predicates, false);
    joinOrderEnumerator.appendASPJoin(c, *plan, *scanE2Plan);
    auto scanE3Plan = createRelScanPlan(e3, post, predicates, false);
    joinOrderEnumerator.appendASPJoin(post, *plan, *scanE3Plan);
    compileHashJoinWithNode(*plan, op, predicates);
    projectionEnumerator.enumerateProjectionBody(*queryPart->getProjectionBody(), *plan);
    plan->setExpressionsToCollect(queryPart->getProjectionBody()->getProjectionExpressions());
    return plan;
}

unique_ptr<LogicalPlan> Enumerator::getIS6Plan(const BoundStatement& statement) {
    auto queryPart = extractQueryPart(statement);
    auto queryGraph = queryPart->getQueryGraph(0);
    auto predicates = queryPart->getQueryGraphPredicate(0)->splitOnAND();
    auto comment = queryGraph->getQueryNode(0);
    assert(comment->getRawName() == "comment");
    auto post = queryGraph->getQueryNode(1);
    assert(post->getRawName() == "post");
    auto f = queryGraph->getQueryNode(2);
    assert(f->getRawName() == "f");
    auto p = queryGraph->getQueryNode(3);
    assert(p->getRawName() == "p");
    auto e1 = queryGraph->getQueryRel(0);
    assert(e1->getRawName() == "e1");
    auto e2 = queryGraph->getQueryRel(1);
    assert(e2->getRawName() == "e2");
    auto e3 = queryGraph->getQueryRel(2);
    assert(e3->getRawName() == "e3");
    
    auto plan = createRelScanPlan(e1, comment, predicates, true);
    auto scanE2Plan = createRelScanPlan(e2, post, predicates, false);
    joinOrderEnumerator.appendASPJoin(post, *plan, *scanE2Plan);
    compileHashJoinWithNode(*plan, f, predicates);
    auto scanE3Plan = createRelScanPlan(e3, f, predicates, false);
    joinOrderEnumerator.appendASPJoin(f, *plan, *scanE3Plan);
    compileHashJoinWithNode(*plan, p, predicates);
    projectionEnumerator.enumerateProjectionBody(*queryPart->getProjectionBody(), *plan);
    plan->setExpressionsToCollect(queryPart->getProjectionBody()->getProjectionExpressions());
    return plan;
}

unique_ptr<LogicalPlan> Enumerator::getIS7Plan(const BoundStatement& statement) {
    auto queryPart = extractQueryPart(statement);
    auto queryGraph = queryPart->getQueryGraph(0);
    auto predicates = queryPart->getQueryGraphPredicate(0)->splitOnAND();
    auto mAuth = queryGraph->getQueryNode(0);
    assert(mAuth->getRawName() == "mAuth");
    auto cmt0 = queryGraph->getQueryNode(1);
    assert(cmt0->getRawName() == "cmt0");
    auto cmt1 = queryGraph->getQueryNode(2);
    assert(cmt1->getRawName() == "cmt1");
    auto rAuth = queryGraph->getQueryNode(3);
    assert(rAuth->getRawName() == "rAuth");
    auto e1 = queryGraph->getQueryRel(0);
    assert(e1->getRawName() == "e1");
    auto e2 = queryGraph->getQueryRel(1);
    assert(e2->getRawName() == "e2");
    auto e3 = queryGraph->getQueryRel(2);
    assert(e3->getRawName() == "e3");

    auto plan = createRelScanPlan(e1, cmt0, predicates, true);
    joinOrderEnumerator.appendExtend(
        *e2, e2->getSrcNodeName() == cmt0->getUniqueName() ? FWD : BWD, *plan);
    compileHashJoinWithNode(*plan, cmt1, predicates);
    auto scanE3Plan = createRelScanPlan(e3, cmt1, predicates, false);
    joinOrderEnumerator.appendASPJoin(cmt1, *plan, *scanE3Plan);
    compileHashJoinWithNode(*plan, rAuth, predicates);
    projectionEnumerator.enumerateProjectionBody(*queryPart->getProjectionBody(), *plan);
    plan->setExpressionsToCollect(queryPart->getProjectionBody()->getProjectionExpressions());
    return plan;
}

// (a)-[e1]->(b)-[e2]->(c)-[e3]->(d)
unique_ptr<LogicalPlan> Enumerator::getThreeHopPlan(const BoundStatement& statement) {
    auto queryPart = extractQueryPart(statement);
    auto queryGraph = queryPart->getQueryGraph(0);
    auto predicates = queryPart->getQueryGraphPredicate(0)->splitOnAND();
    auto a = queryGraph->getQueryNode(0);
    assert(a->getRawName() == "a");
    auto b = queryGraph->getQueryNode(1);
    assert(b->getRawName() == "b");
    auto c = queryGraph->getQueryNode(2);
    assert(c->getRawName() == "c");
    auto d = queryGraph->getQueryNode(3);
    assert(d->getRawName() == "d");
    auto e1 = queryGraph->getQueryRel(0);
    assert(e1->getRawName() == "e1");
    auto e2 = queryGraph->getQueryRel(1);
    assert(e2->getRawName() == "e2");
    auto e3 = queryGraph->getQueryRel(2);
    assert(e3->getRawName() == "e3");
    //******* plan compilation ************

    // compile a-e1-b-e2-c
    auto plan = createRelScanPlan(e1, b, predicates, true);
    compileHashJoinWithNode(*plan, a, predicates);
    joinOrderEnumerator.appendExtend(
        *e2, e2->getSrcNodeName() == b->getUniqueName() ? FWD : BWD, *plan);
    compileHashJoinWithNode(*plan, c, predicates);
    // hash join with e3
    auto scanE3Plan = createRelScanPlan(e3, c, predicates, false);
    if (ENABLE_ASP) {
        joinOrderEnumerator.appendASPJoin(c, *plan, *scanE3Plan);
    } else {
        joinOrderEnumerator.appendHashJoin(c, *plan, *scanE3Plan);
    }
    compileHashJoinWithNode(*plan, d, predicates);
    projectionEnumerator.enumerateProjectionBody(*queryPart->getProjectionBody(), *plan);
    plan->setExpressionsToCollect(queryPart->getProjectionBody()->getProjectionExpressions());
    return plan;
}

NormalizedQueryPart* Enumerator::extractQueryPart(const BoundStatement& statement) {
    assert(statement.getStatementType() == StatementType::QUERY);
    auto& regularQuery = (BoundRegularQuery&)statement;
    assert(regularQuery.getNumSingleQueries() == 1);
    auto singleQuery = regularQuery.getSingleQuery(0);
    propertiesToScan.clear();
    for (auto& expression : singleQuery->getPropertiesToRead()) {
        assert(expression->expressionType == PROPERTY);
        propertiesToScan.push_back(expression);
    }
    assert(singleQuery->getNumQueryParts() == 1);
    return singleQuery->getQueryPart(0);
}

unique_ptr<LogicalPlan> Enumerator::createRelScanPlan(shared_ptr<RelExpression> rel,
    shared_ptr<NodeExpression>& boundNode, expression_vector& predicates, bool isScanNodeTable) {
    auto plan = make_unique<LogicalPlan>();
    joinOrderEnumerator.appendScanNodeID(boundNode, *plan);
    if (isScanNodeTable) {
        auto predicatesToApply = extractPredicatesForNode(predicates, *boundNode);
        joinOrderEnumerator.planFiltersForNode(predicatesToApply, *boundNode, *plan);
        joinOrderEnumerator.planPropertyScansForNode(*boundNode, *plan);
    }
    auto direction = rel->getSrcNodeName() == boundNode->getUniqueName() ? FWD : BWD;
    joinOrderEnumerator.appendExtend(*rel, direction, *plan);
    return plan;
}

void Enumerator::compileHashJoinWithNode(
    LogicalPlan& plan, shared_ptr<NodeExpression>& node, expression_vector& predicates) {
    auto buildPlan = make_unique<LogicalPlan>();
    joinOrderEnumerator.appendScanNodeID(node, *buildPlan);
    auto predicatesToApply = extractPredicatesForNode(predicates, *node);
    joinOrderEnumerator.planFiltersForNode(predicatesToApply, *node, *buildPlan);
    joinOrderEnumerator.planPropertyScansForNode(*node, *buildPlan);
    if (ENABLE_ASP) {
        joinOrderEnumerator.appendASPJoin(node, plan, *buildPlan);
    } else {
        joinOrderEnumerator.appendHashJoin(node, plan, *buildPlan);
    }
}

} // namespace planner
} // namespace graphflow