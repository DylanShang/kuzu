#include "function/decimal/vector_decimal_functions.h"

#include "common/exception/overflow.h"
#include "function/arithmetic/add.h"
#include "function/arithmetic/subtract.h"
#include "function/arithmetic/modulo.h"
#include "function/cast/functions/numeric_limits.h"
#include "function/scalar_function.h"

using namespace kuzu::common;
using std::max;
using std::min;

namespace kuzu {
namespace function {

using param_get_func_t = std::function<std::pair<int, int>(int, int, int, int)>;

struct DecimalAdd {
    template<typename A, typename B, typename R>
    static inline void operation(A& left, B& right, R& result, common::ValueVector& resultValueVector) {
        constexpr auto pow10s = pow10Sequence<R>();
        auto precision = DecimalType::getPrecision(resultValueVector.dataType);
        // todo(Maxwell): Instead of using decimaltype::getprecision, consider something more efficient
        if (pow10s[precision] - right <= left) {
            throw OverflowException("Decimal Addition result is out of range");
        }
        result = left + right;
    }
};

struct DecimalSubtract {
    template<typename A, typename B, typename R>
    static inline void operation(A& left, B& right, R& result, common::ValueVector& resultValueVector) {
        constexpr auto pow10s = pow10Sequence<R>();
        auto precision = DecimalType::getPrecision(resultValueVector.dataType);
        if (-pow10s[precision] + right >= left) {
            throw OverflowException("Decimal Subtraction result is out of range");
        }
        result = left - right;
    }
};

struct DecimalMultiply {
    template<typename A, typename B, typename R>
    static inline void operation(A& left, B& right, R& result, common::ValueVector& resultValueVector) {
        constexpr auto pow10s = pow10Sequence<R>();
        auto precision = DecimalType::getPrecision(resultValueVector.dataType);
        auto scale = DecimalType::getScale(resultValueVector.dataType);
        auto lim1 = std::abs(pow10s[precision + scale] / right); // will logically overflow?
        auto lim2Low = NumericLimits<R>::maximum() / right; // will physically overflow?
        auto lim2High = NumericLimits<R>::minimum() / right;
        if (left >= lim1 || left <= -lim1 || left >= lim2Low || left <= lim2High) {
            throw OverflowException("Overflow encountered when attempting to multiply decimals");
        }
        result = (left * right + (scale > 0 ? 5 * pow10s[scale-1] : 0)) / pow10s[scale];
    }
};

struct DecimalDivide {
    template<typename A, typename B, typename R>
    static inline void operation(A& left, B& right, R& result, common::ValueVector& resultValueVector) {
        constexpr auto pow10s = pow10Sequence<R>();
        auto precision = DecimalType::getPrecision(resultValueVector.dataType);
        auto scale = DecimalType::getScale(resultValueVector.dataType);
        if (left >= NumericLimits<R>::maximum() / pow10s[scale] || left <= NumericLimits<R>::minimum() / pow10s[scale]) {
            throw OverflowException("Overflow encountered when attempting to divide decimals");
        }
        result = (left * pow10s[scale] + (scale > 0 ? 5 * pow10s[scale-1] : 0)) / right;
    }
};

struct DecimalModulo {
    template<typename A, typename B, typename R>
    static inline void operation(A& left, B& right, R& result, common::ValueVector&) {
        result = left % right;   
    }
};

struct DecimalNegate {
    template<typename A, typename R>
    static inline void operation(A& input, R& result, common::ValueVector&) {
        result = -input;
    }
};

struct DecimalAbs {
    template<typename A, typename R>
    static inline void operation(A& input, R& result, common::ValueVector&) {
        result = input;
        if (result < 0) {
            result = -result;
        }
    }
};

struct DecimalFloor {
    template<typename A, typename R>
    static inline void operation(A& input, R& result, common::ValueVector& resultValueVector) {
        auto scale = DecimalType::getScale(resultValueVector.dataType);
        if (input < 0) {
            // round to larger absolute value
            result = input + (input % pow10s[scale]);
        } else {
            // round to smaller absolute value
            result = input - (input % pow10s[scale]);
        }
    }
};

struct DecimalCeil {
    template<typename A, typename R>
    static inline void operation(A& input, R& result, common::ValueVector& resultValueVector) {
        auto scale = DecimalType::getScale(resultValueVector.dataType);
        if (input < 0) {
            // round to smaller absolute value
            result = input - (input % pow10s[scale]);
        } else {
            // round to larger absolute value
            result = input + (input % pow10s[scale]);
        }
    }
};

template<typename FUNC>
static std::unique_ptr<FunctionBindData> genericBinaryArithmeticFunc(
    const binder::expression_vector& arguments, Function* func, param_get_func_t getParams) {
    auto asScalar = ku_dynamic_cast<Function*, ScalarFunction*>(func);
    KU_ASSERT(asScalar != nullptr);
    auto resultingType = arguments[0]->getDataType().copy();
    if (arguments[0]->getDataType() != arguments[1]->getDataType()) {
        auto precision1 = DecimalType::getPrecision(arguments[0]->getDataType());
        auto precision2 = DecimalType::getPrecision(arguments[1]->getDataType());
        auto scale1 = DecimalType::getScale(arguments[0]->getDataType());
        auto scale2 = DecimalType::getScale(arguments[1]->getDataType());
        auto params = getParams(precision1, precision2, scale1, scale2);
        resultingType = LogicalType::DECIMAL(params.first, params.second);
    }
    switch (resultingType->getPhysicalType()) {
    case PhysicalTypeID::INT16:
        asScalar->execFunc = ScalarFunction::BinaryStringExecFunction<int16_t, int16_t, int16_t, FUNC>;
        break;
    case PhysicalTypeID::INT32:
        asScalar->execFunc = ScalarFunction::BinaryStringExecFunction<int32_t, int32_t, int32_t, FUNC>;
        break;
    case PhysicalTypeID::INT64:
        asScalar->execFunc = ScalarFunction::BinaryStringExecFunction<int64_t, int64_t, int64_t, FUNC>;
        break;
    case PhysicalTypeID::INT128:
        asScalar->execFunc = ScalarFunction::BinaryStringExecFunction<int128_t, int128_t, int128_t, FUNC>;
        break;
    default:
        KU_UNREACHABLE;
    }
    return std::make_unique<FunctionBindData>(std::vector<LogicalType>{*resultingType, *resultingType}, std::move(resultingType));
}

template<typename FUNC>
static std::unique_ptr<FunctionBindData> genericUnaryArithmeticFunc(
    const binder::expression_vector& arguments, Function* func) {
    auto asScalar = ku_dynamic_cast<Function*, ScalarFunction*>(func);
    KU_ASSERT(asScalar != nullptr);
    auto resultingType = arguments[0]->getDataType().copy();
    switch(resultingType.getPhysicalType()) {
        case PhysicalTypeID::INT16:
            asScalar->execFunc = ScalarFunction::UnaryStringExecFunction<int16_t, int16_t, FUNC>;
            break;
        case PhysicalTypeID::INT32:
            asScalar->execFunc = ScalarFunction::UnaryStringExecFunction<int32_t, int32_t, FUNC>;
            break;
        case PhysicalTypeID::INT64:
            asScalar->execFunc = ScalarFunction::UnaryStringExecFunction<int64_t, int64_t, FUNC>;
            break;
        case PhysicalTypeID::INT128:
            asScalar->execFunc = ScalarFunction::UnaryStringExecFunction<int128_t, int128_t, FUNC>;
            break;
        default:
            KU_UNREACHABLE;
    }
    return std::make_unique<FunctionBindData>(std::vector<LogicalType>{*resultingType}, std::move(resultingType));
}

// Very basic decimal addition rules
static std::pair<int, int> resultingGeneralParams(int p1, int p2, int s1, int s2) {
    auto s = max(s1, s2);
    auto p = max({p1, p2, max(p1 - s1, p2 - s2) + s});
    return {p, s};
}

// Following param func rules are from
// https://learn.microsoft.com/en-us/sql/t-sql/data-types/precision-scale-and-length-transact-sql
// todo: Figure out which param rules we should use
static std::pair<int, int> resultingAddParams(int p1, int p2, int s1, int s2) {
    auto p = min(DECIMAL_PRECISION_LIMIT, max(s1, s2) + max(p1 - s1, p2 - s2) + 1);
    auto s = min(p, max(s1, s2));
    if (max(p1 - s1, p2 - s2) < min(DECIMAL_PRECISION_LIMIT, p) - s) {
        s = min(p, DECIMAL_PRECISION_LIMIT) - max(p1 - s1, p2 - s2);
    }
    return {p, s};
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindAddFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericBinaryArithmeticFunc<DecimalAdd>(arguments, func, resultingAddParams);
}

static std::pair<int, int> resultingSubtractParams(int p1, int p2, int s1, int s2) {
    auto p = min(DECIMAL_PRECISION_LIMIT, max(s1, s2) + max(p1 - s1, p2 - s2) + 1);
    auto s = min(p, max(s1, s2));
    if (max(p1 - s1, p2 - s2) < min(DECIMAL_PRECISION_LIMIT, p) - s) {
        s = min(p, DECIMAL_PRECISION_LIMIT) - max(p1 - s1, p2 - s2);
    }
    return {p, s};
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindSubtractFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericBinaryArithmeticFunc<DecimalSubtract>(arguments, func, resultingSubtractParams);
}

static std::pair<int, int> resultingMultiplyParams(int p1, int p2, int s1, int s2) {
    if (p1 + p2 + 1 > DECIMAL_PRECISION_LIMIT) {
        throw OverflowException("Resulting precision of decimal multiplication greater than 38");
    }
    auto p = p1 + p2 + 1;
    auto s = s1 + s2;
    return {p, s};
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindMultiplyFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericBinaryArithmeticFunc<DecimalMultiply>(arguments, func, resultingMultiplyParams);
}

static std::pair<int, int> resultingDivideParams(int p1, int p2, int s1, int s2) {
    auto p = min(DECIMAL_PRECISION_LIMIT, p1 - s1 + s2 + max(6, s1 + p2 + 1));
    auto s = min(p, max(6, s1 + p2 + 1)); // todo: complete rules
    return {p, s};
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindDivideFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericBinaryArithmeticFunc<DecimalDivide>(arguments, func, resultingDivideParams);
}

static std::pair<int, int> resultingModuloParams(int p1, int p2, int s1, int s2) {
    auto p = min(DECIMAL_PRECISION_LIMIT, min(p1 - s1, p2 - s2) + max(s1, s2));
    auto s = min(p, max(s1, s2));
    return {p, s};
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindModuloFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericBinaryArithmeticFunc<DecimalModulo>(arguments, func, resultingModuloParams);
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindNegateFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericUnaryArithmeticFunc<DecimalNegate>(arguments, func);
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindFloorFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericUnaryArithmeticFunc<DecimalFloor>(arguments, func);
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindCeilFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericUnaryArithmeticFunc<DecimalCeil>(arguments, func);

}

} // namespace function
} // namespace kuzu