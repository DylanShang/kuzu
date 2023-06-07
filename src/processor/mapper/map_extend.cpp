#include "planner/logical_plan/logical_operator/logical_extend.h"
#include "planner/logical_plan/logical_operator/logical_recursive_extend.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/recursive_extend/shortest_path_recursive_join.h"
#include "processor/operator/recursive_extend/variable_length_recursive_join.h"
#include "processor/operator/scan/generic_scan_rel_tables.h"
#include "processor/operator/scan/scan_rel_table_columns.h"
#include "processor/operator/scan/scan_rel_table_lists.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

static std::vector<property_id_t> populatePropertyIds(
    table_id_t relID, const expression_vector& properties) {
    std::vector<property_id_t> outputColumns;
    for (auto& expression : properties) {
        auto propertyExpression = (PropertyExpression*)expression.get();
        outputColumns.push_back(propertyExpression->getPropertyID(relID));
    }
    return outputColumns;
}

static std::unique_ptr<RelTableCollection> populateRelTableCollection(table_id_t boundNodeTableID,
    const RelExpression& rel, RelDirection direction, const expression_vector& properties,
    const RelsStore& relsStore, const catalog::Catalog& catalog) {
    std::vector<DirectedRelTableData*> tables;
    std::vector<std::unique_ptr<RelTableScanState>> tableScanStates;
    for (auto relTableID : rel.getTableIDs()) {
        auto relTableSchema = catalog.getReadOnlyVersion()->getRelTableSchema(relTableID);
        if (relTableSchema->getBoundTableID(direction) != boundNodeTableID) {
            continue;
        }
        tables.push_back(relsStore.getRelTable(relTableID)->getDirectedTableData(direction));
        std::vector<property_id_t> propertyIds;
        for (auto& property : properties) {
            auto propertyExpression = reinterpret_cast<PropertyExpression*>(property.get());
            propertyIds.push_back(propertyExpression->hasPropertyID(relTableID) ?
                                      propertyExpression->getPropertyID(relTableID) :
                                      INVALID_PROPERTY_ID);
        }
        tableScanStates.push_back(make_unique<RelTableScanState>(std::move(propertyIds),
            relsStore.isSingleMultiplicityInDirection(direction, relTableID) ?
                RelTableDataType::COLUMNS :
                RelTableDataType::LISTS));
    }
    return std::make_unique<RelTableCollection>(std::move(tables), std::move(tableScanStates));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalExtendToPhysical(
    LogicalOperator* logicalOperator) {
    auto extend = (LogicalExtend*)logicalOperator;
    auto outSchema = extend->getSchema();
    auto inSchema = extend->getChild(0)->getSchema();
    auto boundNode = extend->getBoundNode();
    auto nbrNode = extend->getNbrNode();
    auto rel = extend->getRel();
    auto direction = extend->getDirection();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto inNodeIDVectorPos =
        DataPos(inSchema->getExpressionPos(*boundNode->getInternalIDProperty()));
    auto outNodeIDVectorPos =
        DataPos(outSchema->getExpressionPos(*nbrNode->getInternalIDProperty()));
    std::vector<DataPos> outputVectorsPos;
    outputVectorsPos.push_back(outNodeIDVectorPos);
    for (auto& expression : extend->getProperties()) {
        outputVectorsPos.emplace_back(outSchema->getExpressionPos(*expression));
    }
    auto& relsStore = storageManager.getRelsStore();
    if (!rel->isMultiLabeled() && !boundNode->isMultiLabeled()) {
        auto relTableID = rel->getSingleTableID();
        if (relsStore.isSingleMultiplicityInDirection(direction, relTableID)) {
            auto propertyIds = populatePropertyIds(relTableID, extend->getProperties());
            return make_unique<ScanRelTableColumns>(
                relsStore.getRelTable(relTableID)->getDirectedTableData(direction),
                std::move(propertyIds), inNodeIDVectorPos, std::move(outputVectorsPos),
                std::move(prevOperator), getOperatorID(), extend->getExpressionsForPrinting());
        } else {
            assert(!relsStore.isSingleMultiplicityInDirection(direction, relTableID));
            auto propertyIds = populatePropertyIds(relTableID, extend->getProperties());
            return make_unique<ScanRelTableLists>(
                relsStore.getRelTable(relTableID)->getDirectedTableData(direction),
                std::move(propertyIds), inNodeIDVectorPos, std::move(outputVectorsPos),
                std::move(prevOperator), getOperatorID(), extend->getExpressionsForPrinting());
        }
    } else { // map to generic extend
        std::unordered_map<table_id_t, std::unique_ptr<RelTableCollection>>
            relTableCollectionPerNodeTable;
        for (auto boundNodeTableID : boundNode->getTableIDs()) {
            auto relTableCollection = populateRelTableCollection(
                boundNodeTableID, *rel, direction, extend->getProperties(), relsStore, *catalog);
            if (relTableCollection->getNumTablesInCollection() > 0) {
                relTableCollectionPerNodeTable.insert(
                    {boundNodeTableID, std::move(relTableCollection)});
            }
        }
        return std::make_unique<GenericScanRelTables>(inNodeIDVectorPos, outputVectorsPos,
            std::move(relTableCollectionPerNodeTable), std::move(prevOperator), getOperatorID(),
            extend->getExpressionsForPrinting());
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalRecursiveExtendToPhysical(
    planner::LogicalOperator* logicalOperator) {
    auto extend = (LogicalRecursiveExtend*)logicalOperator;
    auto outSchema = extend->getSchema();
    auto inSchema = extend->getChild(0)->getSchema();
    auto boundNode = extend->getBoundNode();
    auto nbrNode = extend->getNbrNode();
    auto rel = extend->getRel();
    auto direction = extend->getDirection();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto inNodeIDVectorPos =
        DataPos(inSchema->getExpressionPos(*boundNode->getInternalIDProperty()));
    auto outNodeIDVectorPos =
        DataPos(outSchema->getExpressionPos(*nbrNode->getInternalIDProperty()));
    auto& relsStore = storageManager.getRelsStore();
    auto relTableID = rel->getSingleTableID();

    auto expressions = inSchema->getExpressionsInScope();
    auto resultCollector = appendResultCollector(expressions, *inSchema, std::move(prevOperator));
    auto sharedInputFTable = resultCollector->getSharedState();
    std::vector<DataPos> outDataPoses;
    std::vector<uint32_t> colIndicesToScan;
    for (auto i = 0u; i < expressions.size(); ++i) {
        outDataPoses.emplace_back(outSchema->getExpressionPos(*expressions[i]));
        colIndicesToScan.push_back(i);
    }
    auto& nodeStore = storageManager.getNodesStore();
    auto nodeTable = nodeStore.getNodeTable(boundNode->getSingleTableID());
    std::string emptyParamString;
    // TODO(Xiyang): this hard code is fine as long as we use just 2 vectors as the intermediate
    // state for recursive join. Though ideally we should construct them from schema.
    auto tmpSrcNodePos = BaseRecursiveJoin::getTmpSrcNodeVectorPos();
    auto scanFrontier =
        std::make_unique<ScanFrontier>(tmpSrcNodePos, getOperatorID(), emptyParamString);
    std::unique_ptr<PhysicalOperator> scanRelTable;
    std::vector<property_id_t> emptyPropertyIDs;
    DataPos tmpDstNodePos{0, 0};
    if (relsStore.isSingleMultiplicityInDirection(direction, relTableID)) {
        tmpDstNodePos = DataPos{0, 1};
        scanRelTable = make_unique<ScanRelTableColumns>(
            relsStore.getRelTable(relTableID)->getDirectedTableData(direction), emptyPropertyIDs,
            tmpSrcNodePos, std::vector<DataPos>{tmpDstNodePos}, std::move(scanFrontier),
            getOperatorID(), emptyParamString);
    } else {
        tmpDstNodePos = DataPos{0, 1};
        scanRelTable = make_unique<ScanRelTableLists>(
            relsStore.getRelTable(relTableID)->getDirectedTableData(direction), emptyPropertyIDs,
            tmpSrcNodePos, std::vector<DataPos>{tmpDstNodePos}, std::move(scanFrontier),
            getOperatorID(), emptyParamString);
    }
    auto sharedState = std::make_shared<RecursiveJoinSharedState>(sharedInputFTable, nodeTable);
    switch (rel->getRelType()) {
    case common::QueryRelType::SHORTEST: {
        auto distanceVectorPos =
            DataPos{outSchema->getExpressionPos(*rel->getInternalLengthProperty())};
        return std::make_unique<ShortestPathRecursiveJoin>(rel->getLowerBound(),
            rel->getUpperBound(), nodeTable, sharedState, outDataPoses, colIndicesToScan,
            inNodeIDVectorPos, outNodeIDVectorPos, distanceVectorPos, tmpDstNodePos,
            std::move(resultCollector), getOperatorID(), extend->getExpressionsForPrinting(),
            std::move(scanRelTable));
    }
    /*case common::QueryRelType::VARIABLE_LENGTH: {
        return std::make_unique<VariableLengthRecursiveJoin>(rel->getLowerBound(),
            rel->getUpperBound(), nodeTable, sharedState, outDataPoses, colIndicesToScan,
            inNodeIDVectorPos, outNodeIDVectorPos, tmpDstNodePos, std::move(resultCollector),
            getOperatorID(), extend->getExpressionsForPrinting(), std::move(scanRelTable));
    }*/
    default:
        throw common::NotImplementedException("PlanMapper::mapLogicalRecursiveExtendToPhysical");
    }
}

} // namespace processor
} // namespace kuzu
