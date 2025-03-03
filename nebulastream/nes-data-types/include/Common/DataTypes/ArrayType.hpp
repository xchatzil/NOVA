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

#ifndef NES_DATA_TYPES_INCLUDE_COMMON_DATATYPES_ARRAYTYPE_HPP_
#define NES_DATA_TYPES_INCLUDE_COMMON_DATATYPES_ARRAYTYPE_HPP_

#include <Common/DataTypes/DataType.hpp>

namespace NES {

/**
 * @brief Arrays con be constructed of any built-in type.
 * Arrays always have a fixed sized and can not be extended.
 */
// Todo #4906: remove or reenable https://github.com/nebulastream/nebulastream/issues/4907
class ArrayType : public DataType {
  public:
    /**
     * @brief Constructs a new ArrayType.
     * @param length length of the array
     * @param component component type
     */
    inline ArrayType(uint64_t length, DataTypePtr component) noexcept : length(length), component(std::move(component)) {}

    ~ArrayType() override = default;

    /**
     * @brief Checks if this data type is an ArrayType.
     */
    [[nodiscard]] bool isArray() const final { return true; }

    /**
     * @brief Checks if this data type is an ArrayType.
     */
    [[nodiscard]] bool isCharArray() const final { return component->isChar(); }

    /**
     * @brief Checks if two data types are equal.
     * @param otherDataType
     * @return
     */
    bool equals(DataTypePtr otherDataType) final;

    /**
     * @brief Calculates the joined data type between this data type and the other.
     * If they have no possible joined data type, the coined type is Undefined.
     * @param other data type
     * @return DataTypePtr joined data type
     */
    DataTypePtr join(DataTypePtr otherDataType) final;

    /**
    * @brief Returns a string representation of the data type.
    * @return string
    */
    std::string toString() final;

    uint64_t const length;
    DataTypePtr const component;
};

}// namespace NES

#endif// NES_DATA_TYPES_INCLUDE_COMMON_DATATYPES_ARRAYTYPE_HPP_
