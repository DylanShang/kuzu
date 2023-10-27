#pragma once

#include "function/scalar_function.h"

namespace kuzu {
namespace function {

struct VectorListFunction : public VectorFunction {
    template<typename OPERATION, typename RESULT_TYPE>
    static scalar_exec_func getBinaryListExecFuncSwitchRight(common::LogicalType rightType) {
        scalar_exec_func execFunc;
        switch (rightType.getPhysicalType()) {
        case common::PhysicalTypeID::BOOL: {
            execFunc =
                BinaryExecListStructFunction<common::list_entry_t, uint8_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT64: {
            execFunc =
                BinaryExecListStructFunction<common::list_entry_t, int64_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT32: {
            execFunc =
                BinaryExecListStructFunction<common::list_entry_t, int32_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT16: {
            execFunc =
                BinaryExecListStructFunction<common::list_entry_t, int16_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT8: {
            execFunc =
                BinaryExecListStructFunction<common::list_entry_t, int8_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::UINT64: {
            execFunc = BinaryExecListStructFunction<common::list_entry_t, uint64_t, RESULT_TYPE,
                OPERATION>;
        } break;
        case common::PhysicalTypeID::UINT32: {
            execFunc = BinaryExecListStructFunction<common::list_entry_t, uint32_t, RESULT_TYPE,
                OPERATION>;
        } break;
        case common::PhysicalTypeID::UINT16: {
            execFunc = BinaryExecListStructFunction<common::list_entry_t, uint16_t, RESULT_TYPE,
                OPERATION>;
        } break;
        case common::PhysicalTypeID::UINT8: {
            execFunc =
                BinaryExecListStructFunction<common::list_entry_t, uint8_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT128: {
            execFunc = BinaryExecListStructFunction<common::list_entry_t, common::int128_t,
                RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::DOUBLE: {
            execFunc = BinaryExecListStructFunction<common::list_entry_t, double_t, RESULT_TYPE,
                OPERATION>;
        } break;
        case common::PhysicalTypeID::FLOAT: {
            execFunc =
                BinaryExecListStructFunction<common::list_entry_t, float_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::STRING: {
            execFunc = BinaryExecListStructFunction<common::list_entry_t, common::ku_string_t,
                RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INTERVAL: {
            execFunc = BinaryExecListStructFunction<common::list_entry_t, common::interval_t,
                RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INTERNAL_ID: {
            execFunc = BinaryExecListStructFunction<common::list_entry_t, common::internalID_t,
                RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::VAR_LIST: {
            execFunc = BinaryExecListStructFunction<common::list_entry_t, common::list_entry_t,
                RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::STRUCT: {
            execFunc = BinaryExecListStructFunction<common::list_entry_t, common::struct_entry_t,
                RESULT_TYPE, OPERATION>;
        } break;
        default: {
            throw common::NotImplementedException{
                "VectorListFunctions::getBinaryListOperationDefinition"};
        }
        }
        return execFunc;
    }

    template<typename OPERATION, typename RESULT_TYPE>
    static scalar_exec_func getBinaryListExecFuncSwitchAll(common::LogicalType type) {
        scalar_exec_func execFunc;
        switch (type.getPhysicalType()) {
        case common::PhysicalTypeID::INT64: {
            execFunc = BinaryExecListStructFunction<int64_t, int64_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT32: {
            execFunc = BinaryExecListStructFunction<int32_t, int32_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT16: {
            execFunc = BinaryExecListStructFunction<int16_t, int16_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT8: {
            execFunc = BinaryExecListStructFunction<int8_t, int8_t, RESULT_TYPE, OPERATION>;
        } break;
        default: {
            throw common::NotImplementedException{
                "VectorListFunctions::getBinaryListOperationDefinition"};
        }
        }
        return execFunc;
    }

    template<typename OPERATION, typename RESULT_TYPE>
    static scalar_exec_func getTernaryListExecFuncSwitchAll(common::LogicalType type) {
        scalar_exec_func execFunc;
        switch (type.getPhysicalType()) {
        case common::PhysicalTypeID::INT64: {
            execFunc =
                TernaryExecListStructFunction<int64_t, int64_t, int64_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT32: {
            execFunc =
                TernaryExecListStructFunction<int32_t, int32_t, int32_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT16: {
            execFunc =
                TernaryExecListStructFunction<int16_t, int16_t, int16_t, RESULT_TYPE, OPERATION>;
        } break;
        case common::PhysicalTypeID::INT8: {
            execFunc =
                TernaryExecListStructFunction<int8_t, int8_t, int8_t, RESULT_TYPE, OPERATION>;
        } break;
        default: {
            throw common::NotImplementedException{
                "VectorListFunctions::getTernaryListOperationDefinition"};
        }
        }
        return execFunc;
    }

    template<typename OPERATION>
    static std::unique_ptr<FunctionBindData> bindFuncListAggre(
        const binder::expression_vector& arguments, Function* definition) {
        auto vectorFunctionDefinition = reinterpret_cast<ScalarFunction*>(definition);
        auto resultType = common::VarListType::getChildType(&arguments[0]->dataType);
        switch (resultType->getLogicalTypeID()) {
        case common::LogicalTypeID::SERIAL:
        case common::LogicalTypeID::INT64: {
            vectorFunctionDefinition->execFunc =
                UnaryExecListStructFunction<common::list_entry_t, int64_t, OPERATION>;
        } break;
        case common::LogicalTypeID::INT32: {
            vectorFunctionDefinition->execFunc =
                UnaryExecListStructFunction<common::list_entry_t, int32_t, OPERATION>;
        } break;
        case common::LogicalTypeID::INT16: {
            vectorFunctionDefinition->execFunc =
                UnaryExecListStructFunction<common::list_entry_t, int16_t, OPERATION>;
        } break;
        case common::LogicalTypeID::INT8: {
            vectorFunctionDefinition->execFunc =
                UnaryExecListStructFunction<common::list_entry_t, int8_t, OPERATION>;
        } break;
        case common::LogicalTypeID::UINT64: {
            vectorFunctionDefinition->execFunc =
                UnaryExecListStructFunction<common::list_entry_t, uint64_t, OPERATION>;
        } break;
        case common::LogicalTypeID::UINT32: {
            vectorFunctionDefinition->execFunc =
                UnaryExecListStructFunction<common::list_entry_t, uint32_t, OPERATION>;
        } break;
        case common::LogicalTypeID::UINT16: {
            vectorFunctionDefinition->execFunc =
                UnaryExecListStructFunction<common::list_entry_t, uint16_t, OPERATION>;
        } break;
        case common::LogicalTypeID::UINT8: {
            vectorFunctionDefinition->execFunc =
                UnaryExecListStructFunction<common::list_entry_t, uint8_t, OPERATION>;
        } break;
        case common::LogicalTypeID::INT128: {
            vectorFunctionDefinition->execFunc =
                UnaryExecListStructFunction<common::list_entry_t, common::int128_t, OPERATION>;
        } break;
        case common::LogicalTypeID::DOUBLE: {
            vectorFunctionDefinition->execFunc =
                UnaryExecListStructFunction<common::list_entry_t, double_t, OPERATION>;
        } break;
        case common::LogicalTypeID::FLOAT: {
            vectorFunctionDefinition->execFunc =
                UnaryExecListStructFunction<common::list_entry_t, float_t, OPERATION>;
        } break;
        default: {
            throw common::NotImplementedException(definition->name + "::bindFunc");
        }
        }
        return std::make_unique<FunctionBindData>(*resultType);
    }
};

struct ListCreationVectorFunction : public VectorListFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* definition);
    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result);
};

struct ListRangeVectorFunction : public VectorListFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* definition);
};

struct SizeVectorFunction : public VectorListFunction {
    static function_set getFunctionSet();
};

struct ListExtractVectorFunction : public VectorListFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* definition);
};

struct ListConcatVectorFunction : public VectorListFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* definition);
};

struct ListAppendVectorFunction : public VectorListFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* definition);
};

struct ListPrependVectorFunction : public VectorListFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* definition);
};

struct ListPositionVectorFunction : public VectorListFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* definition);
};

struct ListContainsVectorFunction : public VectorListFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* definition);
};

struct ListSliceVectorFunction : public VectorListFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* definition);
};

struct ListSortVectorFunction : public VectorListFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* definition);
    template<typename T>
    static void getExecFunction(const binder::expression_vector& arguments, scalar_exec_func& func);
};

struct ListReverseSortVectorFunction : public VectorListFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* definition);
    template<typename T>
    static void getExecFunction(const binder::expression_vector& arguments, scalar_exec_func& func);
};

struct ListSumVectorFunction : public VectorListFunction {
    static function_set getFunctionSet();
};

struct ListProductVectorFunction : public VectorListFunction {
    static function_set getFunctionSet();
};

struct ListDistinctVectorFunction : public VectorListFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* definition);
};

struct ListUniqueVectorFunction : public VectorListFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* definition);
};

struct ListAnyValueVectorFunction : public VectorListFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* definition);
};

} // namespace function
} // namespace kuzu
