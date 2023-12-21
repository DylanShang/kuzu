#include "common/file_system/file_info.h"

#include "common/file_system/file_system.h"

#if defined(_WIN32)
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace kuzu {
namespace common {

FileInfo::~FileInfo() {
#ifdef _WIN32
    if (handle != nullptr) {
        CloseHandle((HANDLE)handle);
    }
#else
    if (fd != -1) {
        close(fd);
    }
#endif
}

uint64_t FileInfo::getFileSize() {
    return fileSystem->getFileSize(this);
}

void FileInfo::readFromFile(void* buffer, uint64_t numBytes, uint64_t position) {
    fileSystem->readFromFile(this, buffer, numBytes, position);
}

int64_t FileInfo::readFile(void* buf, size_t nbyte) {
    return fileSystem->readFile(this, buf, nbyte);
}

void FileInfo::writeFile(const uint8_t* buffer, uint64_t numBytes, uint64_t offset) {
    fileSystem->writeFile(this, buffer, numBytes, offset);
}

void FileInfo::writeFileAsync(const uint8_t* buffer, uint64_t numBytes, uint64_t offset, uv_loop_t* loop, NodeGroupInfo* info) {
    fileSystem->writeFileAsync(this, buffer, numBytes, offset, loop, info);
}

int64_t FileInfo::seek(uint64_t offset, int whence) {
    return fileSystem->seek(this, offset, whence);
}

void FileInfo::truncate(uint64_t size) {
    fileSystem->truncate(this, size);
}

} // namespace common
} // namespace kuzu
