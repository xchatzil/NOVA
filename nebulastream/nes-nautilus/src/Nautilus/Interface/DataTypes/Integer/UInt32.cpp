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
#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/DataTypes/InvocationPlugin.hpp>

namespace NES::Nautilus {

UInt32::UInt32(uint32_t value) : Int(&type), value(value){};
Nautilus::IR::Types::StampPtr UInt32::getType() const { return Nautilus::IR::Types::StampFactory::createUInt32Stamp(); }
std::shared_ptr<Any> UInt32::copy() { return create<UInt32>(value); }
const std::shared_ptr<Int> UInt32::add(const Int& other) const {
    auto& otherValue = other.staticCast<UInt32>();
    return create<UInt32>(value + otherValue.value);
}

const std::shared_ptr<Int> UInt32::sub(const Int& other) const {
    auto& otherValue = other.staticCast<UInt32>();
    return create<UInt32>(value - otherValue.value);
}
const std::shared_ptr<Int> UInt32::div(const Int& other) const {
    auto& otherValue = other.staticCast<UInt32>();
    return create<UInt32>(value / otherValue.value);
}
const std::shared_ptr<Int> UInt32::mod(const Int& other) const {
    auto& otherValue = other.staticCast<UInt32>();
    return create<UInt32>(value % otherValue.value);
}
const std::shared_ptr<Int> UInt32::mul(const Int& other) const {
    auto& otherValue = other.staticCast<UInt32>();
    return create<UInt32>(value * otherValue.value);
}
const std::shared_ptr<Boolean> UInt32::equals(const Int& other) const {
    auto& otherValue = other.staticCast<UInt32>();
    return create<Boolean>(value == otherValue.value);
}
const std::shared_ptr<Boolean> UInt32::lessThan(const Int& other) const {
    auto& otherValue = other.staticCast<UInt32>();
    return create<Boolean>(value < otherValue.value);
}
const std::shared_ptr<Boolean> UInt32::greaterThan(const Int& other) const {
    auto& otherValue = other.staticCast<UInt32>();
    return create<Boolean>(value > otherValue.value);
}
const std::shared_ptr<Int> UInt32::bitWiseAnd(const Int& other) const {
    auto& otherValue = other.staticCast<UInt32>();
    return create<UInt32>(value & otherValue.value);
}
const std::shared_ptr<Int> UInt32::bitWiseOr(const Int& other) const {
    auto& otherValue = other.staticCast<UInt32>();
    return create<UInt32>(value | otherValue.value);
}
const std::shared_ptr<Int> UInt32::bitWiseXor(const Int& other) const {
    auto& otherValue = other.staticCast<UInt32>();
    return create<UInt32>(value ^ otherValue.value);
}
const std::shared_ptr<Int> UInt32::bitWiseLeftShift(const Int& other) const {
    auto& otherValue = other.staticCast<UInt32>();
    return create<UInt32>(value << otherValue.value);
}
const std::shared_ptr<Int> UInt32::bitWiseRightShift(const Int& other) const {
    auto& otherValue = other.staticCast<UInt32>();
    return create<UInt32>(value >> otherValue.value);
}

uint32_t UInt32::getValue() const { return value; }
int64_t UInt32::getRawInt() const { return value; }
std::string UInt32::toString() { return std::to_string(value); }
}// namespace NES::Nautilus
