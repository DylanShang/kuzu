#pragma once

#include <memory>
#include <vector>

#include "src/common/include/configs.h"

using namespace std;

namespace graphflow {
namespace storage {

class BufferManager;

class FileHandle {
    friend class BufferManager;

public:
    FileHandle(string path, uint64_t numPages);

    inline void unswizzle(uint32_t pageIdx) { pageToFrameMap[pageIdx] = UINT64_MAX; }
    inline uint64_t getPageIdxEntry(uint32_t pageIdx) { return pageToFrameMap[pageIdx]; }
    inline void swizzle(uint32_t pageIdx, uint64_t swizzledVal) {
        pageToFrameMap[pageIdx] = swizzledVal;
    }

    void readPage(char *frame, uint64_t pageIdx);

private:
    int fileDescriptor;
    vector<uint64_t> pageToFrameMap;
};

} // namespace storage
} // namespace graphflow
