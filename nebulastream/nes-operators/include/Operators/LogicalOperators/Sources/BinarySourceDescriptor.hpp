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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_BINARYSOURCEDESCRIPTOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_BINARYSOURCEDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical binary source
 */
class BinarySourceDescriptor : public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(SchemaPtr schema, std::string filePath);
    static SourceDescriptorPtr create(SchemaPtr schema, std::string sourceName, std::string filePath);

    /**
     * @brief Get the path of binary file
     * @return
     */
    const std::string& getFilePath() const;

    [[nodiscard]] bool equal(SourceDescriptorPtr const& other) const override;

    std::string toString() const override;

    SourceDescriptorPtr copy() override;

  private:
    explicit BinarySourceDescriptor(SchemaPtr schema, std::string filePath);
    explicit BinarySourceDescriptor(SchemaPtr schema, std::string sourceName, std::string filePath);

    std::string filePath;
};

using BinarySourceDescriptorPtr = std::shared_ptr<BinarySourceDescriptor>;

}// namespace NES

#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_BINARYSOURCEDESCRIPTOR_HPP_
