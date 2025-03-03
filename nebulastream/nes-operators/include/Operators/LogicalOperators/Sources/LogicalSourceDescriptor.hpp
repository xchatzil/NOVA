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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_LOGICALSOURCEDESCRIPTOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_LOGICALSOURCEDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining referencing a logical source.
 */
class LogicalSourceDescriptor : public SourceDescriptor {

  public:
    /**
     * @brief Factory method to create a new logical source descriptor.
     * @param logicalSourceName Name of this source
     * @return SourceDescriptorPtr
     */
    static SourceDescriptorPtr create(std::string logicalSourceName);

    [[nodiscard]] bool equal(SourceDescriptorPtr const& other) const override;

    std::string toString() const override;
    SourceDescriptorPtr copy() override;

  private:
    explicit LogicalSourceDescriptor(std::string logicalSourceName);
};

using LogicalSourceDescriptorPtr = std::shared_ptr<LogicalSourceDescriptor>;

}// namespace NES

#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_LOGICALSOURCEDESCRIPTOR_HPP_
