#include "processor/result/merge_hash_table.h"

namespace kuzu {
namespace processor {

MergeHashTable::MergeHashTable(storage::MemoryManager& memoryManager,
    std::vector<common::LogicalType> keyDataTypes,
    std::vector<common::LogicalType> dependentKeyDataTypes,
    const std::vector<std::unique_ptr<function::AggregateFunction>>& aggregateFunctions,
    uint64_t numEntriesToAllocate, std::unique_ptr<FactorizedTableSchema> tableSchema)
    : AggregateHashTable(memoryManager, keyDataTypes, dependentKeyDataTypes, aggregateFunctions,
          numEntriesToAllocate, std::move(tableSchema)) {
    distinctColIdxInFT = hashColIdxInFT - 1;
}

uint64_t MergeHashTable::matchFTEntries(const std::vector<common::ValueVector*>& flatKeyVectors,
    const std::vector<common::ValueVector*>& unFlatKeyVectors, uint64_t numMayMatches,
    uint64_t numNoMatches) {
    auto colIdx = 0u;
    for (auto& flatKeyVector : flatKeyVectors) {
        numMayMatches =
            matchFlatVecWithFTColumn(flatKeyVector, numMayMatches, numNoMatches, colIdx++);
    }
    for (auto& unFlatKeyVector : unFlatKeyVectors) {
        numMayMatches =
            matchUnFlatVecWithFTColumn(unFlatKeyVector, numMayMatches, numNoMatches, colIdx++);
    }
    for (auto i = 0u; i < numMayMatches; i++) {
        noMatchIdxes[numNoMatches++] = mayMatchIdxes[i];
        onMatchSlotIdxes.emplace(mayMatchIdxes[i]);
    }
    return numNoMatches;
}

void MergeHashTable::initializeFTEntries(const std::vector<common::ValueVector*>& flatKeyVectors,
    const std::vector<common::ValueVector*>& unFlatKeyVectors,
    const std::vector<common::ValueVector*>& dependentKeyVectors,
    uint64_t numFTEntriesToInitialize) {
    AggregateHashTable::initializeFTEntries(
        flatKeyVectors, unFlatKeyVectors, dependentKeyVectors, numFTEntriesToInitialize);
    for (auto i = 0u; i < numFTEntriesToInitialize; i++) {
        auto entryIdx = entryIdxesToInitialize[i];
        auto entry = hashSlotsToUpdateAggState[entryIdx]->entry;
        auto onMatch = !onMatchSlotIdxes.contains(entryIdx);
        onMatchSlotIdxes.erase(entryIdx);
        factorizedTable->updateFlatCellNoNull(entry, distinctColIdxInFT, &onMatch /* isOnMatch */);
    }
}

} // namespace processor
} // namespace kuzu