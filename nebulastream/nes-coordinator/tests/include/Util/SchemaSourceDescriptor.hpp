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

#ifndef NES_COORDINATOR_TESTS_INCLUDE_UTIL_SCHEMASOURCEDESCRIPTOR_HPP_
#define NES_COORDINATOR_TESTS_INCLUDE_UTIL_SCHEMASOURCEDESCRIPTOR_HPP_

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <utility>
namespace NES {

class SchemaSourceDescriptor : public SourceDescriptor {
  public:
    static SourceDescriptorPtr create(const SchemaPtr&& schema) { return std::make_shared<SchemaSourceDescriptor>(schema); }
    explicit SchemaSourceDescriptor(SchemaPtr schema) : SourceDescriptor(std::move(schema)) {}
    std::string toString() const override { return "Schema Source Descriptor"; }
    bool equal(SourceDescriptorPtr const& other) const override { return other->getSchema()->equals(this->getSchema()); }
    ~SchemaSourceDescriptor() override = default;
    SourceDescriptorPtr copy() override { return create(schema->copy()); };
};

}// namespace NES

#endif// NES_COORDINATOR_TESTS_INCLUDE_UTIL_SCHEMASOURCEDESCRIPTOR_HPP_
