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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Expressions/BinaryExpressionNode.hpp>
#include <Expressions/ConstantValueExpressionNode.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>
#include <Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Operators/Exceptions/TypeInferenceException.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Util/Logger/Logger.hpp>
#include <unordered_set>
#include <utility>

namespace NES {

LogicalJoinOperator::LogicalJoinOperator(Join::LogicalJoinDescriptorPtr joinDefinition, OperatorId id, OriginId originId)
    : Operator(id), LogicalBinaryOperator(id), OriginIdAssignmentOperator(id, originId),
      joinDefinition(std::move(joinDefinition)) {}

bool LogicalJoinOperator::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<LogicalJoinOperator>()->getId() == id;
}

std::string LogicalJoinOperator::toString() const {
    std::stringstream ss;
    ss << "Join(" << id << ")";
    return ss.str();
}

Join::LogicalJoinDescriptorPtr LogicalJoinOperator::getJoinDefinition() const { return joinDefinition; }

bool LogicalJoinOperator::inferSchema() {

    if (!LogicalBinaryOperator::inferSchema()) {
        return false;
    }

    //validate that only two different type of schema were present
    if (distinctSchemas.size() != 2) {
        throw TypeInferenceException(
            fmt::format("LogicalJoinOperator: Found {} distinct schemas but expected 2 distinct schemas.",
                        distinctSchemas.size()));
    }
    // first step: clear the schemas
    leftInputSchema->clear();
    rightInputSchema->clear();
    //  join has two logical children each with it own schema, we ensure in LogicalBinaryOperator that left = distinctSchema[0]
    leftInputSchema->copyFields(distinctSchemas[0]); //
    rightInputSchema->copyFields(distinctSchemas[1]);// and right = [1]

    //reset left and right schema
    leftInputSchema->clear();
    rightInputSchema->clear();

    NES_DEBUG("LogicalJoinOperator: Iterate over all ExpressionNode to if check join field is in schema.");
    // Maintain a list of visited nodes as there are multiple root nodes
    std::unordered_set<std::shared_ptr<BinaryExpressionNode>> visitedExpressions;
    auto bfsIterator = BreadthFirstNodeIterator(joinDefinition->getJoinExpression());
    for (auto itr = bfsIterator.begin(); itr != BreadthFirstNodeIterator::end(); ++itr) {
        if ((*itr)->instanceOf<BinaryExpressionNode>()) {
            auto visitingOp = (*itr)->as<BinaryExpressionNode>();
            if (visitedExpressions.contains(visitingOp)) {
                // skip rest of the steps as the node found in already visited node list
                continue;
            } else {
                visitedExpressions.insert(visitingOp);
                if (!(*itr)->as<BinaryExpressionNode>()->getLeft()->instanceOf<BinaryExpressionNode>()) {
                    //Find the schema for left and right join key
                    const auto leftJoinKey = (*itr)->as<BinaryExpressionNode>()->getLeft()->as<FieldAccessExpressionNode>();
                    const auto leftJoinKeyName = leftJoinKey->getFieldName();
                    const auto foundLeftKey = findSchemaInDistinctSchemas(*leftJoinKey, leftInputSchema);
                    NES_ASSERT_THROW_EXCEPTION(foundLeftKey,
                                               TypeInferenceException,
                                               "LogicalJoinOperator: Unable to find left join key " + leftJoinKeyName
                                                   + " in schemas.");
                    const auto rightJoinKey = (*itr)->as<BinaryExpressionNode>()->getRight()->as<FieldAccessExpressionNode>();
                    const auto rightJoinKeyName = rightJoinKey->getFieldName();
                    const auto foundRightKey = findSchemaInDistinctSchemas(*rightJoinKey, rightInputSchema);
                    NES_ASSERT_THROW_EXCEPTION(foundRightKey,
                                               TypeInferenceException,
                                               "LogicalJoinOperator: Unable to find right join key " + rightJoinKeyName
                                                   + " in schemas.");
                    NES_DEBUG("LogicalJoinOperator: Inserting operator in collection of already visited node.");
                    visitedExpressions.insert(visitingOp);
                }
            }
        }
    }
    // Clearing now the distinct schemas
    distinctSchemas.clear();

    // Checking if left and right input schema are not empty and are not equal
    NES_ASSERT_THROW_EXCEPTION(leftInputSchema->getSchemaSizeInBytes() > 0,
                               TypeInferenceException,
                               "LogicalJoinOperator: left schema is emtpy");
    NES_ASSERT_THROW_EXCEPTION(rightInputSchema->getSchemaSizeInBytes() > 0,
                               TypeInferenceException,
                               "LogicalJoinOperator: right schema is emtpy");
    NES_ASSERT_THROW_EXCEPTION(!rightInputSchema->equals(leftInputSchema, false),
                               TypeInferenceException,
                               "LogicalJoinOperator: Found both left and right input schema to be same.");

    //Infer stamp of window definition
    const auto windowType = joinDefinition->getWindowType()->as<Windowing::TimeBasedWindowType>();
    windowType->inferStamp(leftInputSchema);

    //Reset output schema and add fields from left and right input schema
    outputSchema->clear();
    const auto& sourceNameLeft = leftInputSchema->getQualifierNameForSystemGeneratedFields();
    const auto& sourceNameRight = rightInputSchema->getQualifierNameForSystemGeneratedFields();
    const auto& newQualifierForSystemField = sourceNameLeft + sourceNameRight;

    windowStartFieldName = newQualifierForSystemField + "$start";
    windowEndFieldName = newQualifierForSystemField + "$end";
    outputSchema->addField(createField(windowStartFieldName, BasicType::UINT64));
    outputSchema->addField(createField(windowEndFieldName, BasicType::UINT64));

    // create dynamic fields to store all fields from left and right sources
    for (const auto& field : leftInputSchema->fields) {
        outputSchema->addField(field->getName(), field->getDataType());
    }

    for (const auto& field : rightInputSchema->fields) {
        outputSchema->addField(field->getName(), field->getDataType());
    }

    NES_DEBUG("Output schema for join={}", outputSchema->toString());
    joinDefinition->updateOutputDefinition(outputSchema);
    joinDefinition->updateSourceTypes(leftInputSchema, rightInputSchema);
    return true;
}

OperatorPtr LogicalJoinOperator::copy() {
    auto copy = LogicalOperatorFactory::createJoinOperator(joinDefinition, id)->as<LogicalJoinOperator>();
    copy->setLeftInputOriginIds(leftInputOriginIds);
    copy->setRightInputOriginIds(rightInputOriginIds);
    copy->setLeftInputSchema(leftInputSchema);
    copy->setRightInputSchema(rightInputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setOriginId(originId);
    copy->windowStartFieldName = windowStartFieldName;
    copy->windowEndFieldName = windowEndFieldName;
    copy->setOperatorState(operatorState);
    copy->setStatisticId(statisticId);
    for (const auto& [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

bool LogicalJoinOperator::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<LogicalJoinOperator>()) {
        auto rhsJoin = rhs->as<LogicalJoinOperator>();
        return joinDefinition->getWindowType()->equal(rhsJoin->joinDefinition->getWindowType())
            && joinDefinition->getJoinExpression()->equal(rhsJoin->joinDefinition->getJoinExpression())
            && joinDefinition->getOutputSchema()->equals(rhsJoin->joinDefinition->getOutputSchema())
            && joinDefinition->getRightSourceType()->equals(rhsJoin->joinDefinition->getRightSourceType())
            && joinDefinition->getLeftSourceType()->equals(rhsJoin->joinDefinition->getLeftSourceType());
    }
    return false;
}

void LogicalJoinOperator::inferStringSignature() {
    OperatorPtr operatorNode = shared_from_this()->as<Operator>();
    NES_TRACE("LogicalJoinOperator: Inferring String signature for {}", operatorNode->toString());
    NES_ASSERT(!children.empty() && children.size() == 2, "LogicalJoinOperator: Join should have 2 children.");
    //Infer query signatures for child operators
    for (const auto& child : children) {
        const LogicalOperatorPtr childOperator = child->as<LogicalOperator>();
        childOperator->inferStringSignature();
    }

    std::stringstream signatureStream;
    signatureStream << "WINDOW-DEFINITION=" << joinDefinition->getWindowType()->toString() << ",";

    auto rightChildSignature = children[0]->as<LogicalOperator>()->getHashBasedSignature();
    auto leftChildSignature = children[1]->as<LogicalOperator>()->getHashBasedSignature();
    signatureStream << *rightChildSignature.begin()->second.begin() + ").";
    signatureStream << *leftChildSignature.begin()->second.begin();

    //Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}

std::vector<OriginId> LogicalJoinOperator::getOutputOriginIds() const { return OriginIdAssignmentOperator::getOutputOriginIds(); }

void LogicalJoinOperator::setOriginId(OriginId originId) {
    OriginIdAssignmentOperator::setOriginId(originId);
    joinDefinition->setOriginId(originId);
}

const ExpressionNodePtr LogicalJoinOperator::getJoinExpression() const { return joinDefinition->getJoinExpression(); }

const std::string& LogicalJoinOperator::getWindowStartFieldName() const { return windowStartFieldName; }

const std::string& LogicalJoinOperator::getWindowEndFieldName() const { return windowEndFieldName; }

void LogicalJoinOperator::setWindowStartEndKeyFieldName(std::string_view windowStartFieldName,
                                                        std::string_view windowEndFieldName) {
    this->windowStartFieldName = windowStartFieldName;
    this->windowEndFieldName = windowEndFieldName;
}

bool LogicalJoinOperator::findSchemaInDistinctSchemas(FieldAccessExpressionNode& joinKey, const SchemaPtr& inputSchema) {
    for (auto itr = distinctSchemas.begin(); itr != distinctSchemas.end();) {
        bool fieldExistsInSchema;
        const auto joinKeyName = joinKey.getFieldName();
        // If field name contains qualifier
        if (joinKeyName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) != std::string::npos) {
            fieldExistsInSchema = (*itr)->contains(joinKeyName);
        } else {
            fieldExistsInSchema = ((*itr)->getField(joinKeyName) != nullptr);
        }
        if (fieldExistsInSchema) {
            inputSchema->copyFields(*itr);
            joinKey.inferStamp(inputSchema);
            distinctSchemas.erase(itr);
            return true;
        }
        ++itr;
    }
    if (distinctSchemas.empty()) {
        joinKey.inferStamp(inputSchema);
        return true;
    }
    return false;
}

}// namespace NES
