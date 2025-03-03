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
#include <Operators/LogicalOperators/StatisticCollection/LogicalStatisticWindowOperator.hpp>
#include <Types/WindowType.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::Statistic {

LogicalStatisticWindowOperator::LogicalStatisticWindowOperator(OperatorId id,
                                                               Windowing::WindowTypePtr windowType,
                                                               WindowStatisticDescriptorPtr windowStatisticDescriptor,
                                                               StatisticMetricHash metricHash,
                                                               SendingPolicyPtr sendingPolicy,
                                                               TriggerConditionPtr triggerCondition)
    : Operator(id), LogicalUnaryOperator(id), windowType(std::move(windowType)),
      windowStatisticDescriptor(std::move(windowStatisticDescriptor)), metricHash(metricHash), sendingPolicy(sendingPolicy),
      triggerCondition(triggerCondition) {}

bool LogicalStatisticWindowOperator::inferSchema() {
    using enum BasicType;
    if (!LogicalUnaryOperator::inferSchema()) {
        return false;
    }

    // Inferring the stamp for the windowType and the fieldToTrackStatisticsOver that is part of the descriptor
    windowType->inferStamp(inputSchema);
    windowStatisticDescriptor->inferStamps(inputSchema);

    // Creating output schema
    const auto qualifierNameWithSeparator = inputSchema->getQualifierNameForSystemGeneratedFieldsWithSeparator();
    outputSchema->clear();
    outputSchema->addField(qualifierNameWithSeparator + BASE_FIELD_NAME_START, UINT64);
    outputSchema->addField(qualifierNameWithSeparator + BASE_FIELD_NAME_END, UINT64);
    outputSchema->addField(qualifierNameWithSeparator + STATISTIC_HASH_FIELD_NAME, UINT64);
    outputSchema->addField(qualifierNameWithSeparator + STATISTIC_TYPE_FIELD_NAME, UINT64);
    outputSchema->addField(qualifierNameWithSeparator + OBSERVED_TUPLES_FIELD_NAME, UINT64);
    windowStatisticDescriptor->addDescriptorFields(*outputSchema, qualifierNameWithSeparator);

    NES_DEBUG("OutputSchema is = {}", outputSchema->toString());

    return true;
}

bool LogicalStatisticWindowOperator::equal(const NodePtr& rhs) const {
    if (rhs->instanceOf<LogicalStatisticWindowOperator>()) {
        auto rhsStatisticOperatorNode = rhs->as<LogicalStatisticWindowOperator>();
        return windowType->equal(rhsStatisticOperatorNode->windowType) && statisticId == rhsStatisticOperatorNode->statisticId
            && windowStatisticDescriptor->equal(rhsStatisticOperatorNode->windowStatisticDescriptor)
            && metricHash == rhsStatisticOperatorNode->metricHash && *sendingPolicy == *rhsStatisticOperatorNode->sendingPolicy
            && *triggerCondition == *rhsStatisticOperatorNode->triggerCondition;
    }
    return false;
}

bool LogicalStatisticWindowOperator::isIdentical(const NodePtr& rhs) const {
    return equal(rhs) && rhs->as<LogicalStatisticWindowOperator>()->getId() == id;
}

std::string LogicalStatisticWindowOperator::toString() const {
    return fmt::format("LogicalStatisticWindowOperator({}, {}): Windowtype: {} Descriptor: {} InputOriginIds: {} MetricHash: {} "
                       "SendingPolicy: {} TriggerCondition: {}",
                       id,
                       statisticId,
                       windowType->toString(),
                       windowStatisticDescriptor->toString(),
                       fmt::join(inputOriginIds.begin(), inputOriginIds.end(), ", "),
                       metricHash,
                       sendingPolicy->toString(),
                       triggerCondition->toString());
}

OperatorPtr LogicalStatisticWindowOperator::copy() {
    auto copy = LogicalOperatorFactory::createStatisticBuildOperator(windowType,
                                                                     windowStatisticDescriptor,
                                                                     metricHash,
                                                                     sendingPolicy,
                                                                     triggerCondition,
                                                                     id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setOperatorState(operatorState);
    copy->setStatisticId(statisticId);
    copy->setInputOriginIds(inputOriginIds);
    for (const auto& [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

Windowing::WindowTypePtr LogicalStatisticWindowOperator::getWindowType() const { return windowType; }

WindowStatisticDescriptorPtr LogicalStatisticWindowOperator::getWindowStatisticDescriptor() const {
    return windowStatisticDescriptor;
}

SendingPolicyPtr LogicalStatisticWindowOperator::getSendingPolicy() const { return sendingPolicy; }

TriggerConditionPtr LogicalStatisticWindowOperator::getTriggerCondition() const { return triggerCondition; }

void LogicalStatisticWindowOperator::inferStringSignature() {
    auto op = shared_from_this()->as<Operator>();
    NES_TRACE("StatisticWindowOperatorNode: Inferring String signature for {}", op->toString());

    std::stringstream signatureStream;
    signatureStream << "STATISTIC_BUILD_OPERATOR(" + windowStatisticDescriptor->toString() + ").";

    //Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}

StatisticMetricHash LogicalStatisticWindowOperator::getMetricHash() const { return metricHash; }

}// namespace NES::Statistic
