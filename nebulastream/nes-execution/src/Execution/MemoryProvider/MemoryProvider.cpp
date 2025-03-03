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

#include <API/Schema.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Execution/MemoryProvider/ColumnMemoryProvider.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Runtime::Execution::MemoryProvider {

Nautilus::TextValue* loadAssociatedTextValue(void* tupleBuffer, uint32_t childIndex) {
    auto tb = TupleBuffer::reinterpretAsTupleBuffer(tupleBuffer);
    auto childBuffer = tb.loadChildBuffer(childIndex);
    return Nautilus::TextValue::load(childBuffer);
}

Nautilus::Value<> MemoryProvider::load(const PhysicalTypePtr& type,
                                       Nautilus::Value<Nautilus::MemRef>& bufferReference,
                                       Nautilus::Value<Nautilus::MemRef>& fieldReference) const {
    if (type->isBasicType()) {
        auto basicType = std::static_pointer_cast<BasicPhysicalType>(type);
        switch (basicType->nativeType) {
            case BasicPhysicalType::NativeType::BOOLEAN: {
                return fieldReference.load<Nautilus::Boolean>();
            };
            case BasicPhysicalType::NativeType::INT_8: {
                return fieldReference.load<Nautilus::Int8>();
            };
            case BasicPhysicalType::NativeType::INT_16: {
                return fieldReference.load<Nautilus::Int16>();
            };
            case BasicPhysicalType::NativeType::INT_32: {
                return fieldReference.load<Nautilus::Int32>();
            };
            case BasicPhysicalType::NativeType::INT_64: {
                return fieldReference.load<Nautilus::Int64>();
            };
            case BasicPhysicalType::NativeType::UINT_8: {
                return fieldReference.load<Nautilus::UInt8>();
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                return fieldReference.load<Nautilus::UInt16>();
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                return fieldReference.load<Nautilus::UInt32>();
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                return fieldReference.load<Nautilus::UInt64>();
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                return fieldReference.load<Nautilus::Float>();
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                return fieldReference.load<Nautilus::Double>();
            };
            default: {
                NES_ERROR("Physical Type: {} is currently not supported", type->toString());
                NES_NOT_IMPLEMENTED();
            };
        }
    } else if (type->isArrayType()) {
        NES_ERROR("Physical Type: array type {} is currently not supported", type->toString());
        NES_NOT_IMPLEMENTED();
    } else if (type->isTextType()) {
        auto childIndex = fieldReference.load<Nautilus::UInt32>();
        auto variableSizeBuffer =
            Nautilus::FunctionCall("loadAssociatedTextValue", loadAssociatedTextValue, bufferReference, childIndex);
        return variableSizeBuffer;
    } else {
        NES_ERROR("Physical Type: type {} is currently not supported", type->toString());
        NES_NOT_IMPLEMENTED();
    }
}

uint32_t storeAssociatedTextValue(void* tupleBuffer, const Nautilus::TextValue* textValue) {
    auto tb = TupleBuffer::reinterpretAsTupleBuffer(tupleBuffer);
    auto textBuffer = TupleBuffer::reinterpretAsTupleBuffer((void*) textValue);
    return tb.storeChildBuffer(textBuffer);
}

Nautilus::Value<> MemoryProvider::store(const NES::PhysicalTypePtr& type,
                                        Nautilus::Value<Nautilus::MemRef>& bufferReference,
                                        Nautilus::Value<Nautilus::MemRef>& fieldReference,
                                        Nautilus::Value<>& value) const {
    if (type->isBasicType()) {
        fieldReference.store(value);
        return value;
    } else if (type->isTextType()) {
        auto textValue = value.as<Nautilus::Text>();
        auto childIndex = Nautilus::FunctionCall("storeAssociatedTextValue",
                                                 storeAssociatedTextValue,
                                                 bufferReference,
                                                 textValue->getReference());
        fieldReference.store(childIndex);
        return value;
    }
    NES_NOT_IMPLEMENTED();
}

bool MemoryProvider::includesField(const std::vector<Nautilus::Record::RecordFieldIdentifier>& projections,
                                   const Nautilus::Record::RecordFieldIdentifier& fieldIndex) const {
    if (projections.empty()) {
        return true;
    }
    return std::find(projections.begin(), projections.end(), fieldIndex) != projections.end();
}

MemoryProvider::~MemoryProvider() {}

MemoryProviderPtr MemoryProvider::createMemoryProvider(const uint64_t bufferSize, const SchemaPtr schema) {
    if (schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT) {
        auto rowMemoryLayout = MemoryLayouts::RowLayout::create(schema, bufferSize);
        return std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(rowMemoryLayout);
    } else if (schema->getLayoutType() == Schema::MemoryLayoutType::COLUMNAR_LAYOUT) {
        auto columnMemoryLayout = MemoryLayouts::ColumnLayout::create(schema, bufferSize);
        return std::make_unique<Runtime::Execution::MemoryProvider::ColumnMemoryProvider>(columnMemoryLayout);
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

}// namespace NES::Runtime::Execution::MemoryProvider
