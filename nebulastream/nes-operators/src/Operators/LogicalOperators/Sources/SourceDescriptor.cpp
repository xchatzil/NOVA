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

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <utility>

namespace NES {

SourceDescriptor::SourceDescriptor(SchemaPtr schema) { this->schema = schema->copy(); }

SourceDescriptor::SourceDescriptor(SchemaPtr schema, std::string logicalSourceName)
    : schema(std::move(schema)), logicalSourceName(std::move(logicalSourceName)) {}

SourceDescriptor::SourceDescriptor(SchemaPtr schema, std::string logicalSourceName, std::string physicalSourceName)
    : schema(std::move(schema)), logicalSourceName(std::move(logicalSourceName)),
      physicalSourceName(std::move(physicalSourceName)) {}

SchemaPtr SourceDescriptor::getSchema() const { return schema; }

std::string SourceDescriptor::getLogicalSourceName() const { return logicalSourceName; }

std::string SourceDescriptor::getPhysicalSourceName() const { return physicalSourceName; }

void SourceDescriptor::setSchema(const SchemaPtr& schema) { this->schema = schema; }

void SourceDescriptor::setPhysicalSourceName(std::string_view physicalSourceName) {
    this->physicalSourceName = physicalSourceName;
}

}// namespace NES
