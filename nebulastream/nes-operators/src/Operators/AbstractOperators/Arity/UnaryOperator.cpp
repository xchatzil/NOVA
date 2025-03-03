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
#include <Identifiers/NESStrongTypeFormat.hpp>
#include <Operators/AbstractOperators/Arity/UnaryOperator.hpp>
#include <Util/OperatorsUtil.hpp>
#include <fmt/format.h>

namespace NES {

UnaryOperator::UnaryOperator(OperatorId id) : Operator(id) {}

void UnaryOperator::setInputSchema(SchemaPtr inputSchema) {
    if (inputSchema) {
        this->inputSchema = std::move(inputSchema);
    }
}

void UnaryOperator::setOutputSchema(SchemaPtr outputSchema) {
    if (outputSchema) {
        this->outputSchema = std::move(outputSchema);
    }
}

SchemaPtr UnaryOperator::getInputSchema() const { return inputSchema; }

SchemaPtr UnaryOperator::getOutputSchema() const { return outputSchema; }

void UnaryOperator::setInputOriginIds(const std::vector<OriginId>& originIds) { this->inputOriginIds = originIds; }

std::vector<OriginId> UnaryOperator::getInputOriginIds() const { return inputOriginIds; }

std::vector<OriginId> UnaryOperator::getOutputOriginIds() const { return inputOriginIds; }

std::string UnaryOperator::toString() const {
    return fmt::format("inputSchema: {}\n"
                       "outputSchema: {}\n"
                       "inputOriginIds: {}",
                       inputSchema->toString(),
                       outputSchema->toString(),
                       fmt::join(inputOriginIds.begin(), inputOriginIds.end(), ", "));
}

}// namespace NES
