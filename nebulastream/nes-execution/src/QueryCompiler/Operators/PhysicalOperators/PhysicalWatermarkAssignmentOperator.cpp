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
#include <Operators/LogicalOperators/Watermarks/WatermarkStrategyDescriptor.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <sstream>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators {

PhysicalWatermarkAssignmentOperator::PhysicalWatermarkAssignmentOperator(
    OperatorId id,
    StatisticId statisticId,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor)
    : Operator(id, statisticId), PhysicalUnaryOperator(id, statisticId, std::move(inputSchema), std::move(outputSchema)),
      watermarkStrategyDescriptor(std::move(watermarkStrategyDescriptor)) {}

PhysicalOperatorPtr
PhysicalWatermarkAssignmentOperator::create(OperatorId id,
                                            StatisticId statisticId,
                                            const SchemaPtr& inputSchema,
                                            const SchemaPtr& outputSchema,
                                            const Windowing::WatermarkStrategyDescriptorPtr& watermarkStrategyDescriptor) {
    return std::make_shared<PhysicalWatermarkAssignmentOperator>(id,
                                                                 statisticId,
                                                                 inputSchema,
                                                                 outputSchema,
                                                                 watermarkStrategyDescriptor);
}

Windowing::WatermarkStrategyDescriptorPtr PhysicalWatermarkAssignmentOperator::getWatermarkStrategyDescriptor() const {
    return watermarkStrategyDescriptor;
}

PhysicalOperatorPtr
PhysicalWatermarkAssignmentOperator::create(StatisticId statisticId,
                                            SchemaPtr inputSchema,
                                            SchemaPtr outputSchema,
                                            Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor) {
    return create(getNextOperatorId(),
                  statisticId,
                  std::move(inputSchema),
                  std::move(outputSchema),
                  std::move(watermarkStrategyDescriptor));
}

std::string PhysicalWatermarkAssignmentOperator::toString() const {
    std::stringstream out;
    out << std::endl;
    out << "PhysicalWatermarkAssignmentOperator:\n";
    out << PhysicalUnaryOperator::toString();
    if (watermarkStrategyDescriptor != nullptr) {
        out << "watermarkStrategyDescriptor: " << watermarkStrategyDescriptor->toString();
    }
    out << std::endl;
    return out.str();
}

OperatorPtr PhysicalWatermarkAssignmentOperator::copy() {
    auto result = create(id, statisticId, inputSchema, outputSchema, getWatermarkStrategyDescriptor());
    result->addAllProperties(properties);
    return result;
}

}// namespace NES::QueryCompilation::PhysicalOperators
