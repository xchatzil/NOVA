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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_CSVSOURCEDESCRIPTOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_CSVSOURCEDESCRIPTOR_HPP_

#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <chrono>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical CSV source
 */
class CsvSourceDescriptor : public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(SchemaPtr schema,
                                      CSVSourceTypePtr csvSourceType,
                                      const std::string& logicalSourceName,
                                      const std::string& physicalSourceName);

    static SourceDescriptorPtr create(SchemaPtr schema, CSVSourceTypePtr csvSourceType);

    /**
     * @brief get source config ptr with all configurations for csv source
     */
    CSVSourceTypePtr getSourceConfig() const;

    [[nodiscard]] bool equal(SourceDescriptorPtr const& other) const override;
    std::string toString() const override;
    SourceDescriptorPtr copy() override;

  private:
    explicit CsvSourceDescriptor(SchemaPtr schema,
                                 CSVSourceTypePtr sourceConfig,
                                 const std::string& logicalSourceName,
                                 const std::string& physicalSourceName);

    CSVSourceTypePtr csvSourceType;
};

using CsvSourceDescriptorPtr = std::shared_ptr<CsvSourceDescriptor>;

}// namespace NES

#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_CSVSOURCEDESCRIPTOR_HPP_
