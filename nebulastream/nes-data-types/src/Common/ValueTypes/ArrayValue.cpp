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
#include <Common/ValueTypes/ArrayValue.hpp>

namespace NES {

std::string ArrayValue::toString() const noexcept { return "ArrayValue"; }

bool ArrayValue::isEquals(ValueTypePtr other) const noexcept {
    return dataType->equals(other->dataType) && values == std::dynamic_pointer_cast<ArrayValue>(other)->values;
}

}// namespace NES
