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

#ifndef NES_DATA_TYPES_INCLUDE_COMMON_DATATYPES_TEXTTYPE_HPP_
#define NES_DATA_TYPES_INCLUDE_COMMON_DATATYPES_TEXTTYPE_HPP_

#include <Common/DataTypes/DataType.hpp>

namespace NES {

/**
 * @brief The Text type represents a variable-sized text field.
 */
class TextType : public DataType {
  public:
    inline TextType() noexcept {}

    ~TextType() override = default;

    [[nodiscard]] bool isText() const override { return true; }

    bool equals(DataTypePtr otherDataType) override;

    DataTypePtr join(DataTypePtr otherDataType) override;

    std::string toString() override;
};

}// namespace NES
#endif// NES_DATA_TYPES_INCLUDE_COMMON_DATATYPES_TEXTTYPE_HPP_
