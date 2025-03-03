/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Common/DataTypes/DataType.hpp>
#include <Common/ValueTypes/BasicValue.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/QuerySignatures/DataTypeToZ3ExprUtil.hpp>
#include <Util/QuerySignatures/Z3ExprAndFieldMap.hpp>
#include <cstring>
#include <z3++.h>

namespace NES::Optimizer {

Z3ExprAndFieldMapPtr
DataTypeToZ3ExprUtil::createForField(const std::string& fieldName, const DataTypePtr& dataType, const z3::ContextPtr& context) {
    NES_DEBUG("DataTypeToZ3ExprUtil: creating z3 expression for field");
    z3::ExprPtr expr;
    if (dataType->isInteger()) {
        expr = std::make_shared<z3::expr>(context->int_const(fieldName.c_str()));
    } else if (dataType->isFloat()) {
        expr = std::make_shared<z3::expr>(context->fpa_const<64>(fieldName.c_str()));
    } else if (dataType->isBoolean()) {
        expr = std::make_shared<z3::expr>(context->bool_const(fieldName.c_str()));
    } else if (dataType->isChar()) {
        expr = std::make_shared<z3::expr>(context->constant(fieldName.c_str(), context->string_sort()));
    } else if (dataType->isCharArray()) {// TODO: Enable Arrays in Z3
        expr = std::make_shared<z3::expr>(context->constant(fieldName.c_str(), context->string_sort()));
    } else {
        NES_THROW_RUNTIME_ERROR("Creating Z3 expression is not possible for " + dataType->toString());
    }
    std::map<std::string, z3::ExprPtr> fieldMap{{fieldName, expr}};
    return Z3ExprAndFieldMap::create(expr, fieldMap);
}

Z3ExprAndFieldMapPtr DataTypeToZ3ExprUtil::createForDataValue(const ValueTypePtr& valueType, const z3::ContextPtr& context) {
    NES_DEBUG("DataTypeToZ3ExprUtil: creating z3 expression for value type");
    if (valueType->dataType->isArray()) {
        NES_THROW_RUNTIME_ERROR("Can't support creating Z3 expression for data value of array type.");
        return nullptr;
    }
    auto basicValueType = std::dynamic_pointer_cast<BasicValue>(valueType);
    auto valueTypeType = basicValueType->dataType;
    z3::ExprPtr expr;
    if (valueTypeType->isUndefined()) {
        NES_THROW_RUNTIME_ERROR("Can't support creating Z3 expression for data value of undefined type.");
    } else if (valueTypeType->isInteger()) {
        expr = std::make_shared<z3::expr>(context->int_val(std::stoi(basicValueType->value)));
    } else if (valueTypeType->isFloat()) {
        expr = std::make_shared<z3::expr>(context->fpa_val(std::stod(basicValueType->value)));
    } else if (valueTypeType->isBoolean()) {
        bool const val =
            (strcasecmp(basicValueType->value.c_str(), "true") == 0 || std::atoi(basicValueType->value.c_str()) != 0);
        expr = std::make_shared<z3::expr>(context->bool_val(val));
    } else if (valueTypeType->isChar()) {
        expr = std::make_shared<z3::expr>(context->string_val(basicValueType->value));
    } else if (valueTypeType->isCharArray()) {// TODO: Enable Arrays for z3.
        expr = std::make_shared<z3::expr>(context->string_val(basicValueType->value));
    } else {
        NES_THROW_RUNTIME_ERROR("Creating Z3 expression is not possible for " + valueTypeType->toString());
    }
    return Z3ExprAndFieldMap::create(expr, {});
}
}// namespace NES::Optimizer
