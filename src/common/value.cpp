#include "src/common/include/value.h"

#include <cassert>

#include "src/common/include/date.h"

using namespace std;

namespace graphflow {
namespace common {

Value& Value::operator=(const Value& other) {
    dataType = other.dataType;
    switch (dataType) {
    case BOOL: {
        val.booleanVal = other.val.booleanVal;
    } break;
    case INT32: {
        val.int32Val = other.val.int32Val;
    } break;
    case DOUBLE: {
        val.doubleVal = other.val.doubleVal;
    } break;
    case STRING: {
        val.strVal = other.val.strVal;
    } break;
    case DATE: {
        val.dateVal = other.val.dateVal;
    } break;
    default:
        assert(false);
    }

    return *this;
}

string Value::toString() const {
    switch (dataType) {
    case BOOL:
        return val.booleanVal == TRUE ? "True" : (val.booleanVal == FALSE ? "False" : "");
    case INT32:
    case INT64:
        return to_string(val.int32Val);
    case DOUBLE:
        return to_string(val.doubleVal);
    case STRING:
        return val.strVal.getAsString();
    case NODE:
        return to_string(val.nodeID.label) + ":" + to_string(val.nodeID.offset);
    case DATE:
        return Date::toString(val.dateVal);
    default:
        assert(false);
    }
}

} // namespace common
} // namespace graphflow
