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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_AGGREGATIONS_WINDOWAGGREGATIONDESCRIPTOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_AGGREGATIONS_WINDOWAGGREGATIONDESCRIPTOR_HPP_

#include <Common/DataTypes/DataType.hpp>
#include <Operators/LogicalOperators/Windows/WindowingForwardRefs.hpp>

namespace NES::Windowing {
/**
 * Abstract class for window aggregations. All window aggregations operate on a field and output another field.
 */
class WindowAggregationDescriptor {
  public:
    enum class Type : uint8_t { Avg, Count, Max, Min, Sum, Median };

    /**
    * Defines the field to which a aggregate output is assigned.
    * @param asField
    * @return WindowAggregationDescriptor
    */
    WindowAggregationDescriptorPtr as(const ExpressionNodePtr& asField);

    /**
    * Returns the result field of the aggregation
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr as() const;

    /**
    * Returns the result field of the aggregation
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr on() const;

    /**
     * @brief Returns the type of this aggregation.
     * @return WindowAggregationDescriptor::Type
     */
    Type getType() const;

    /**
     * @brief Infers the stamp of the expression given the current schema and the typeInferencePhaseContext.
     * @param typeInferencePhaseContext
     * @param schema
     */
    virtual void inferStamp(SchemaPtr schema) = 0;

    /**
    * @brief Creates a deep copy of the window aggregation
    */
    virtual WindowAggregationDescriptorPtr copy() = 0;

    /**
     * @return the input type
     */
    virtual DataTypePtr getInputStamp() = 0;

    /**
     * @return the partial aggregation type
     */
    virtual DataTypePtr getPartialAggregateStamp() = 0;

    /**
     * @return the final aggregation type
     */
    virtual DataTypePtr getFinalAggregateStamp() = 0;

    std::string toString() const;

    std::string getTypeAsString() const;

    /**
     * @brief Check if input window aggregation is equal to this window aggregation definition by checking the aggregation type,
     * on field, and as field
     * @param otherWindowAggregationDescriptor : other window aggregation definition
     * @return true if equal else false
     */
    bool equal(WindowAggregationDescriptorPtr otherWindowAggregationDescriptor) const;

  protected:
    explicit WindowAggregationDescriptor(const FieldAccessExpressionNodePtr& onField);
    WindowAggregationDescriptor(const ExpressionNodePtr& onField, const ExpressionNodePtr& asField);
    WindowAggregationDescriptor() = default;
    ExpressionNodePtr onField;
    ExpressionNodePtr asField;
    Type aggregationType;
};
}// namespace NES::Windowing

#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_AGGREGATIONS_WINDOWAGGREGATIONDESCRIPTOR_HPP_
