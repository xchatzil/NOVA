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

#include <API/AttributeField.hpp>
#include <Expressions/ExpressionNode.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::Windowing {

TimeCharacteristic::TimeCharacteristic(Type type) : type(type), unit(TimeUnit(1)) {}
TimeCharacteristic::TimeCharacteristic(Type type, AttributeFieldPtr field, TimeUnit unit)
    : type(type), field(std::move(field)), unit(std::move(unit)) {}

TimeCharacteristicPtr TimeCharacteristic::createEventTime(ExpressionNodePtr fieldValue, const TimeUnit& unit) {
    if (!fieldValue->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a  {}", fieldValue->toString());
    }
    auto fieldAccess = fieldValue->as<FieldAccessExpressionNode>();
    AttributeFieldPtr keyField = AttributeField::create(fieldAccess->getFieldName(), fieldAccess->getStamp());
    return std::make_shared<TimeCharacteristic>(Type::EventTime, keyField, unit);
}

TimeCharacteristicPtr TimeCharacteristic::createIngestionTime() {
    return std::make_shared<TimeCharacteristic>(Type::IngestionTime);
}

AttributeFieldPtr TimeCharacteristic::getField() const { return field; }

TimeCharacteristic::Type TimeCharacteristic::getType() const { return type; }

TimeUnit TimeCharacteristic::getTimeUnit() const { return unit; }

void TimeCharacteristic::setTimeUnit(const TimeUnit& newUnit) { this->unit = newUnit; }

std::string TimeCharacteristic::toString() const {
    std::stringstream ss;
    ss << "TimeCharacteristic: ";
    ss << " type=" << getTypeAsString();
    if (field) {
        ss << " field=" << field->toString();
    }

    ss << std::endl;
    return ss.str();
}

std::string TimeCharacteristic::getTypeAsString() const {
    switch (type) {
        case Type::IngestionTime: return "IngestionTime";
        case Type::EventTime: return "EventTime";
        default: return "Unknown TimeCharacteristic type";
    }
}

void TimeCharacteristic::setField(AttributeFieldPtr field) { this->field = std::move(field); }

bool TimeCharacteristic::equals(const TimeCharacteristic& other) const {
    const bool equalField = (this->field == nullptr && other.field == nullptr)
        || (this->field != nullptr && other.field != nullptr && this->field->isEqual(other.field));

    return this->type == other.type && equalField && this->unit.equals(other.unit);
}

uint64_t TimeCharacteristic::hash() const {

    uint64_t hashValue = 0;
    hashValue = hashValue * 0x9e3779b1 + std::hash<uint8_t>{}((unsigned char) type);
    if (field) {
        hashValue = hashValue * 0x9e3779b1 + field->hash();
    }
    hashValue = hashValue * 0x9e3779b1 + std::hash<uint64_t>{}(unit.getMillisecondsConversionMultiplier());
    return hashValue;
}

}// namespace NES::Windowing
