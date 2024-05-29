#include "storage/predicate/constant_predicate.h"
#include "storage/compression/compression.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

template <typename T>
bool check(const CompressionMetadata& metadata, ExpressionType expressionType, const Value& value) {
    auto max = metadata.max.get<T>();
    auto min = metadata.min.get<T>();
    auto constant = value.getValue<T>();
    switch (expressionType) {
    case ExpressionType::EQUALS: {
        
    }
    }
}

bool ColumnConstantPredicate::checkCompressionMetadata(const CompressionMetadata& metadata) const {
    switch (value.getDataType()->getPhysicalType()) {
    case PhysicalTypeID::INT8: {

    }
    }
}

}
}
