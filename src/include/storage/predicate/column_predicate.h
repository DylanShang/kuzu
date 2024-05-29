#pragma once

#include <cstdint>
#include "common/cast.h"

namespace kuzu {
namespace storage {

struct CompressionMetadata;

class ColumnPredicate {
public:
    virtual ~ColumnPredicate() = default;

    virtual bool checkCompressionMetadata(const CompressionMetadata& metadata) const = 0;

    virtual std::unique_ptr<ColumnPredicate> copy() const = 0;

    template<class TARGET>
    const TARGET& constCast() const {
        return common::ku_dynamic_cast<const ColumnPredicate&, const TARGET&>(*this);
    }
};



}
}
