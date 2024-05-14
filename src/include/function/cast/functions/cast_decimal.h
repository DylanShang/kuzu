#pragma once

#include <string>
#include "common/types/int128_t.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "function/cast/functions/cast_string_non_nested_functions.h"
#include "function/cast/functions/numeric_limits.h"
#include "function/function.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct CastDecimalTo {
    template<typename SRC, typename DST>
    static void operation(SRC& input, DST& output,
        const ValueVector& inputVec, const ValueVector& outputVec) {
        auto scale = DecimalType::getScale(inputVec.dataType);
        output = input / scale;
    }
};

struct CastToDecimal {
    template<typename SRC, typename DST>
    static void operation(SRC& input, DST& output,
        const ValueVector& inputVec, const ValueVector& outputVec) {
        auto scale = DecimalType::getScale(outputVec.dataType0;)
        output = input * scale;
    }
};

struct CastBetweenDecimal {
    template<typename SRC, typename DST>
    static void operation(SRC& input, DST& output,
        const ValueVector& inputVec, const ValueVector& outputVec) {
        constexpr auto pow10s = pow10Sequence<std::conditional<(sizeof(A) > sizeof(B)), SRC, DST>::type>();
        auto inputPrecision = DecmialType::getPrecision(inputVec.dataType);
        auto outputPrecision = DecmialType::getPrecision(outputVec.dataType);
        auto inputScale = DecmialType::getScale(inputVec.dataType);
        auto outputScale = DecmialType::getScale(outputVec.dataType);
        if (inputScale == outputScale) {
            output = input;
            if (output >= pow10s[outputPrecision] || output <= -pow10s[outputPrecision]) {
                throw OverflowException("Decimal Cast Failed: input {} is not in range of {}",
                    DecimalType::insertDecimalPoint(std::to_string(input), inputScale),
                    outputVec.dataType.toString());
            }
        } else if (inputScale < outputScale) {
            output = input * pow10s[outputScale - inputScale];
        } else if (inputScale > outputScale) {
            output = input / pow10s[inputScale - outputScale];
        }
    }
};

// DECIMAL TO STRING SPECIALIZATION
template<>
inline void CastDecimalTo::operation(int16_t& input, ku_string_t& output,
    const ValueVector& inputVec, const ValueVector&) {
    auto scale = DecimalType::getScale(inputVec.dataType);
    output.set(DecimalType::insertDecimalPoint(std::to_string(input), scale));
}

template<>
inline void CastDecimalTo::operation(int32_t& input, ku_string_t& output,
    const ValueVector& inputVec, const ValueVector&) {
    auto scale = DecimalType::getScale(inputVec.dataType);
    output.set(DecimalType::insertDecimalPoint(std::to_string(input), scale));
}

template<>
inline void CastDecimalTo::operation(int64_t& input, ku_string_t& output,
    const ValueVector& inputVec, const ValueVector&) {
    auto scale = DecimalType::getScale(inputVec.dataType);
    output.set(DecimalType::insertDecimalPoint(std::to_string(input), scale));
}

template<>
inline void CastDecimalTo::operation(int128_t& input, ku_string_t& output,
    const ValueVector& inputVec, const ValueVector&) {
    auto scale = DecimalType::getScale(inputVec.dataType);
    output.set(DecimalType::insertDecimalPoint(Int128_t::ToString(input), scale));
}

// STRING TO DECIMAL SPECIALIZATION
template<>
inline void CastToDecimal::operation(ku_string_t& input, int16_t& output, 
    const ValueVector&, const ValueVector& outputVec) {
    decimalCast((const char*)input.getData(), input.len, output, outputVec.dataType);
}

template<>
inline void CastToDecimal::operation(ku_string_t& input, int32_t& output, 
    const ValueVector&, const ValueVector& outputVec) {
    decimalCast((const char*)input.getData(), input.len, output, outputVec.dataType);
}

template<>
inline void CastToDecimal::operation(ku_string_t& input, int64_t& output, 
    const ValueVector&, const ValueVector& outputVec) {
    decimalCast((const char*)input.getData(), input.len, output, outputVec.dataType);
}

template<>
inline void CastToDecimal::operation(ku_string_t& input, int128_t& output, 
    const ValueVector&, const ValueVector& outputVec) {
    decimalCast((const char*)input.getData(), input.len, output, outputVec.dataType);
}

} // namespace function
} // namespace kuzu
