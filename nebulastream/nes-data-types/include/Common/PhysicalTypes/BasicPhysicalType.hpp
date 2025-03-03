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

#ifndef NES_DATA_TYPES_INCLUDE_COMMON_PHYSICALTYPES_BASICPHYSICALTYPE_HPP_
#define NES_DATA_TYPES_INCLUDE_COMMON_PHYSICALTYPES_BASICPHYSICALTYPE_HPP_

#include <Common/PhysicalTypes/PhysicalType.hpp>
namespace NES {

/**
 * @brief The BasicPhysicalType represents nes data types, which can be directly mapped to a native c++ type.
 */
class BasicPhysicalType final : public PhysicalType {
  public:
    enum class NativeType : uint8_t {
        UINT_8,
        UINT_16,
        UINT_32,
        UINT_64,
        INT_8,
        INT_16,
        INT_32,
        INT_64,
        FLOAT,
        DOUBLE,
        CHAR,
        BOOLEAN,
        UNDEFINED
    };

    /**
     * @brief Constructor for a basic physical type.
     * @param type the data type represented by this physical type
     * @param nativeType the native type of the nes type.
     */
    BasicPhysicalType(DataTypePtr type, NativeType nativeType);

    ~BasicPhysicalType() override = default;

    /**
     * @brief Factory function to create a new physical type.
     * @param type
     * @param nativeType
     * @return PhysicalTypePtr
     */
    static PhysicalTypePtr create(const DataTypePtr& type, NativeType nativeType);

    /**
     * @brief Indicates if this is a basic data type.
     * @return true
     */
    [[nodiscard]] bool isBasicType() const noexcept override { return true; }

    /**
     * @brief Returns the number of bytes occupied by this data type.
     * @return uint64_t
     */
    [[nodiscard]] uint64_t size() const override;

    /**
     * @brief Converts the binary representation of this value to a string.
     * @param rawData a pointer to the raw value
     * @return string
     */
    std::string convertRawToString(void const* rawData) const noexcept override;

    /**
     * @brief Converts the binary representation of this value to a string.
     * @param rawData a pointer to the raw value
     * @return string
    */
    std::string convertRawToStringWithoutFill(void const* rawData) const noexcept override;

    /**
     * @brief Returns the string representation of this physical data type.
     * @return string
     */
    [[nodiscard]] std::string toString() const noexcept override;

    NativeType const nativeType;
};

using BasicPhysicalTypePtr = std::shared_ptr<BasicPhysicalType>;

}// namespace NES

#endif// NES_DATA_TYPES_INCLUDE_COMMON_PHYSICALTYPES_BASICPHYSICALTYPE_HPP_
