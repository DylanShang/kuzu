#include "storage/store/null_column.h"

namespace kuzu {
namespace storage {

struct NullColumnFunc {
    static void readValuesFromPageToVector(const uint8_t* frame, PageCursor& pageCursor,
        ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead,
        const CompressionMetadata& metadata) {
        // Read bit-packed null flags from the frame into the result vector
        // Casting to uint64_t should be safe as long as the page size is a multiple of 8 bytes.
        // Otherwise, it could read off the end of the page.
        if (metadata.isConstant()) {
            auto value = ConstantCompression::getValue<bool>(metadata);
            resultVector->setNullRange(posInVector, numValuesToRead, value);
        } else {
            resultVector->setNullFromBits(
                (uint64_t*)frame, pageCursor.elemPosInPage, posInVector, numValuesToRead);
        }
    }

    static void writeValueToPageFromVector(uint8_t* frame, uint16_t posInFrame, ValueVector* vector,
        uint32_t posInVector, const CompressionMetadata& metadata) {
        if (metadata.isConstant()) {
            // Value to write is identical to the constant value
            return;
        }
        // Casting to uint64_t should be safe as long as the page size is a multiple of 8 bytes.
        // Otherwise, it could read off the end of the page.
        NullMask::setNull((uint64_t*)frame, posInFrame, vector->isNull(posInVector));
    }
};

NullColumn::NullColumn(std::string name, page_idx_t metaDAHPageIdx, BMFileHandle* dataFH,
    BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal, Transaction* transaction,
    RWPropertyStats propertyStatistics, bool enableCompression)
    : Column{name, LogicalType::BOOL(), MetadataDAHInfo{metaDAHPageIdx}, dataFH, metadataFH,
          bufferManager, wal, transaction, propertyStatistics, enableCompression,
          false /*requireNullColumn*/} {
    readToVectorFunc = NullColumnFunc::readValuesFromPageToVector;
    writeFromVectorFunc = NullColumnFunc::writeValueToPageFromVector;
    // Should never be used
    batchLookupFunc = nullptr;
}

void NullColumn::scan(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    if (propertyStatistics.mayHaveNull(*transaction)) {
        scanInternal(transaction, nodeIDVector, resultVector);
    } else {
        resultVector->setAllNonNull();
    }
}

void NullColumn::scan(transaction::Transaction* transaction, node_group_idx_t nodeGroupIdx,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) {
    if (propertyStatistics.mayHaveNull(*transaction)) {
        Column::scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup, resultVector,
            offsetInVector);
    } else {
        resultVector->setNullRange(
            offsetInVector, endOffsetInGroup - startOffsetInGroup, false /*set non-null*/);
    }
}

void NullColumn::scan(transaction::Transaction* transaction, node_group_idx_t nodeGroupIdx,
    ColumnChunk* columnChunk, offset_t startOffset, offset_t endOffset) {
    if (propertyStatistics.mayHaveNull(DUMMY_WRITE_TRANSACTION)) {
        Column::scan(transaction, nodeGroupIdx, columnChunk, startOffset, endOffset);
    } else {
        static_cast<NullColumnChunk*>(columnChunk)->resetToNoNull();
        if (nodeGroupIdx >= metadataDA->getNumElements(transaction->getType())) {
            columnChunk->setNumValues(0);
        } else {
            auto chunkMetadata = metadataDA->get(nodeGroupIdx, transaction->getType());
            auto numValues = chunkMetadata.numValues == 0 ?
                                 0 :
                                 std::min(endOffset, chunkMetadata.numValues - 1) - startOffset + 1;
            columnChunk->setNumValues(numValues);
        }
    }
}

void NullColumn::lookup(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    if (propertyStatistics.mayHaveNull(*transaction)) {
        lookupInternal(transaction, nodeIDVector, resultVector);
    } else {
        for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            resultVector->setNull(pos, false);
        }
    }
}

void NullColumn::append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) {
    auto preScanMetadata = columnChunk->getMetadataToFlush();
    auto startPageIdx = dataFH->addNewPages(preScanMetadata.numPages);
    auto metadata = columnChunk->flushBuffer(dataFH, startPageIdx, preScanMetadata);
    metadataDA->resize(nodeGroupIdx + 1);
    metadataDA->update(nodeGroupIdx, metadata);
    if (static_cast<NullColumnChunk*>(columnChunk)->mayHaveNull()) {
        propertyStatistics.setHasNull(DUMMY_WRITE_TRANSACTION);
    }
}

bool NullColumn::isNull(
    transaction::Transaction* transaction, node_group_idx_t nodeGroupIdx, offset_t offsetInChunk) {
    auto state = getReadState(transaction->getType(), nodeGroupIdx);
    uint64_t result = false;
    if (offsetInChunk >= state.metadata.numValues) {
        return true;
    }
    // Must be aligned to an 8-byte chunk for NullMask read to not overflow
    Column::scan(
        transaction, state, offsetInChunk, offsetInChunk + 1, reinterpret_cast<uint8_t*>(&result));
    return result;
}

void NullColumn::setNull(node_group_idx_t nodeGroupIdx, offset_t offsetInChunk) {
    auto chunkMeta = metadataDA->get(nodeGroupIdx, TransactionType::WRITE);
    propertyStatistics.setHasNull(DUMMY_WRITE_TRANSACTION);
    // Must be aligned to an 8-byte chunk for NullMask read to not overflow
    uint64_t value = true;
    writeValue(chunkMeta, nodeGroupIdx, offsetInChunk, reinterpret_cast<const uint8_t*>(&value));
    if (offsetInChunk >= chunkMeta.numValues) {
        chunkMeta.numValues = offsetInChunk + 1;
        metadataDA->update(nodeGroupIdx, chunkMeta);
    }
}

void NullColumn::write(node_group_idx_t nodeGroupIdx, offset_t offsetInChunk,
    ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    auto chunkMeta = metadataDA->get(nodeGroupIdx, TransactionType::WRITE);
    writeValue(chunkMeta, nodeGroupIdx, offsetInChunk, vectorToWriteFrom, posInVectorToWriteFrom);
    if (vectorToWriteFrom->isNull(posInVectorToWriteFrom)) {
        propertyStatistics.setHasNull(DUMMY_WRITE_TRANSACTION);
    }
    if (offsetInChunk >= chunkMeta.numValues) {
        chunkMeta.numValues = offsetInChunk + 1;
        metadataDA->update(nodeGroupIdx, chunkMeta);
    }
}

bool NullColumn::canCommitInPlace(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    LocalVectorCollection* localChunk, const offset_to_row_idx_t& insertInfo,
    const offset_to_row_idx_t& updateInfo) {
    auto metadata = getMetadata(nodeGroupIdx, transaction->getType());
    if (metadata.compMeta.canAlwaysUpdateInPlace()) {
        return true;
    }
    std::vector<row_idx_t> rowIdxesToRead;
    for (auto& [_, rowIdx] : updateInfo) {
        rowIdxesToRead.push_back(rowIdx);
    }
    for (auto& [_, rowIdx] : insertInfo) {
        rowIdxesToRead.push_back(rowIdx);
    }
    std::sort(rowIdxesToRead.begin(), rowIdxesToRead.end());
    for (auto rowIdx : rowIdxesToRead) {
        auto localVector = localChunk->getLocalVector(rowIdx);
        auto offsetInVector = rowIdx & (DEFAULT_VECTOR_CAPACITY - 1);
        bool value = localVector->getVector()->isNull(offsetInVector);
        if (!metadata.compMeta.canUpdateInPlace(
                reinterpret_cast<const uint8_t*>(&value), 0, dataType->getPhysicalType())) {
            return false;
        }
    }
    return true;
}

void NullColumn::commitLocalChunkInPlace(Transaction* /*transaction*/,
    node_group_idx_t nodeGroupIdx, LocalVectorCollection* localChunk,
    const offset_to_row_idx_t& insertInfo, const offset_to_row_idx_t& updateInfo,
    const offset_set_t& deleteInfo) {
    for (auto& [offsetInChunk, rowIdx] : updateInfo) {
        auto localVector = localChunk->getLocalVector(rowIdx);
        auto offsetInVector = rowIdx & (DEFAULT_VECTOR_CAPACITY - 1);
        write(nodeGroupIdx, offsetInChunk, localVector->getVector(), offsetInVector);
    }
    for (auto& [offsetInChunk, rowIdx] : insertInfo) {
        auto localVector = localChunk->getLocalVector(rowIdx);
        auto offsetInVector = rowIdx & (DEFAULT_VECTOR_CAPACITY - 1);
        write(nodeGroupIdx, offsetInChunk, localVector->getVector(), offsetInVector);
    }
    // Set nulls based on deleteInfo. Note that this code path actually only gets executed when
    // the column is a regular format one. This is not a good design, should be unified with csr
    // one in the future.
    for (auto offsetInChunk : deleteInfo) {
        setNull(nodeGroupIdx, offsetInChunk);
    }
}

} // namespace storage
} // namespace kuzu