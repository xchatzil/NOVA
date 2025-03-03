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

#ifndef NES_DATA_TYPES_INCLUDE_COMMON_DATATYPES_FLOAT_HPP_
#define NES_DATA_TYPES_INCLUDE_COMMON_DATATYPES_FLOAT_HPP_

#include <Common/DataTypes/Numeric.hpp>

namespace NES {

/**
 * @brief Float precision are inexact, variable-precision numeric types.
 * Inexact means that some values cannot be converted exactly to the internal format and are stored as approximations,
 * so that storing and retrieving a value might show slight discrepancies.
 */
class Float final : public Numeric {

  public:
    /**
     * @brief Constructs a new Float type.
     * @param bits the number of bits in which this type is represented.
     * @param lowerBound the lower bound, which is contained in that float.
     * @param upperBound the upper bound, which is contained in that float.
     */
    inline Float(int8_t bits, double lowerBound, double upperBound) noexcept
        : Numeric(bits), lowerBound(lowerBound), upperBound(upperBound) {}

    ~Float() override = default;

    /**
    * @brief Checks if this data type is Float.
    */
    [[nodiscard]] bool isFloat() const override { return true; }

    /**
    * @brief Checks if two data types are equal.
    * @param otherDataType
    * @return
    */
    bool equals(DataTypePtr otherDataType) override;

    /**
    * @brief Calculates the joined data type between this data type and the other.
    * If they have no possible joined data type, the coined type is Undefined.
    * Floats, we can join with all numeric data types.
    * @param other data type
    * @return DataTypePtr joined data type
    */
    DataTypePtr join(DataTypePtr otherDataType) override;

    /**
    * @brief Returns a string representation of the data type.
    * @return string
    */
    std::string toString() override;

    double const lowerBound;
    double const upperBound;
};

}// namespace NES

#endif// NES_DATA_TYPES_INCLUDE_COMMON_DATATYPES_FLOAT_HPP_
