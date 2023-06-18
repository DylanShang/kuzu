#pragma once

#include "common/api.h"
#include "common/types/ku_string.h"

namespace kuzu {
namespace common {

struct blob_t {
    ku_string_t value;
};

struct HexFormatConstants {
    static constexpr const char PREFIX[] = "\\\\x";
    static constexpr const uint64_t PREFIX_LENGTH = 3;
    static constexpr const uint64_t FIRST_BYTE_POS = PREFIX_LENGTH;
    static constexpr const uint64_t SECOND_BYTES_POS = PREFIX_LENGTH + 1;
    static constexpr const uint64_t LENGTH = 5;
    static constexpr const uint64_t NUM_BYTES_TO_SHIFT_FOR_FIRST_BYTE = 4;
    static constexpr const uint64_t SECOND_BYTE_MASK = 0x0F;
    // map of integer -> hex value.
    static constexpr const char* HEX_TABLE = "0123456789ABCDEF";
    // reverse map of byte -> integer value, or -1 for invalid hex values.
    static const int HEX_MAP[256];
};

struct Blob {
    static std::string toString(blob_t& blob);

    static uint64_t getBlobSize(const ku_string_t& blob);

    static void fromString(ku_string_t& str, uint8_t* resultBuffer);

private:
    static void validateHexCode(const uint8_t* blobStr, uint64_t length, uint64_t curPos);
};

} // namespace common
} // namespace kuzu
