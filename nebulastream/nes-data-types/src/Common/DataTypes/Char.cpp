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

#include <Common/DataTypes/Char.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES {

bool Char::equals(DataTypePtr otherDataType) { return otherDataType->isChar(); }

DataTypePtr Char::join(DataTypePtr otherDataType) {
    if (otherDataType->isChar()) {
        return DataTypeFactory::createChar();
    }
    return DataTypeFactory::createUndefined();
}

std::string Char::toString() { return "Char"; }

}// namespace NES
