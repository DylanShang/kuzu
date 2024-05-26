#include "storage/store/table_data.h"

#include "catalog/catalog_entry/table_catalog_entry.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

TableData::TableData(BMFileHandle* dataFH, BMFileHandle* metadataFH, table_id_t tableID,
    const std::string& tableName, BufferManager* bufferManager, WAL* wal, bool enableCompression)
    : dataFH{dataFH}, metadataFH{metadataFH}, tableID{tableID}, tableName{tableName},
      bufferManager{bufferManager}, wal{wal}, enableCompression{enableCompression} {}

void TableData::addColumn(Transaction* transaction, const std::string& colNamePrefix,
    DiskArray<ColumnChunkMetadata>* metadataDA, const MetadataDAHInfo& metadataDAHInfo,
    const catalog::Property& property, ValueVector* defaultValueVector) {
    auto colName = StorageUtils::getColumnName(property.getName(),
        StorageUtils::ColumnType::DEFAULT, colNamePrefix);
    auto column = ColumnFactory::createColumn(colName, *property.getDataType()->copy(),
        metadataDAHInfo, dataFH, metadataFH, bufferManager, wal, transaction, enableCompression);
    column->populateWithDefaultVal(transaction, metadataDA, defaultValueVector);
    columns.push_back(std::move(column));
}

void TableData::prepareCommit() {
    for (auto& column : columns) {
        column->prepareCommit();
    }
}

void TableData::checkpointInMemory() {
    for (auto& column : columns) {
        column->checkpointInMemory();
    }
}

void TableData::rollbackInMemory() {
    for (auto& column : columns) {
        column->rollbackInMemory();
    }
}

} // namespace storage
} // namespace kuzu
