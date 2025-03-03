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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <algorithm>
#include <cmath>
#include <fmt/format.h>

namespace NES {

bool Integer::equals(DataTypePtr otherDataType) {
    if (otherDataType->isInteger()) {
        auto otherInteger = as<Integer>(otherDataType);
        return bits == otherInteger->bits && lowerBound == otherInteger->lowerBound && upperBound == otherInteger->upperBound;
    }
    return false;
}

DataTypePtr Integer::join(DataTypePtr otherDataType) {
    // An integer can be joined with integer types and float types.
    if (otherDataType->isFloat()) {
        // The other type is a float, thus we return a large enough float as a jointed type.
        auto otherFloat = as<Float>(otherDataType);
        auto newBits = std::max(bits, otherFloat->getBits());
        auto newUpperBound = fmax(static_cast<double>(upperBound), otherFloat->upperBound);
        auto newLowerBound = fmin(static_cast<double>(lowerBound), otherFloat->lowerBound);
        return DataTypeFactory::createFloat(newBits, newLowerBound, newUpperBound);
    }
    if (otherDataType->isInteger()) {
        // The other type is an Integer, thus we return a large enough integer.
        auto otherInteger = as<Integer>(otherDataType);
        auto newBits = std::max(bits, otherInteger->getBits());
        auto newUpperBound = std::max(upperBound, otherInteger->upperBound);
        auto newLowerBound = std::min(lowerBound, otherInteger->lowerBound);
        return DataTypeFactory::createInteger(newBits, newLowerBound, newUpperBound);
    }
    return DataTypeFactory::createUndefined();
}

std::string Integer::toString() { return fmt::format("INTEGER({} bits)", bits); }

}// namespace NES
