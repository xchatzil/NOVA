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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JAVAUDF_FLATMAPJAVAUDF_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JAVAUDF_FLATMAPJAVAUDF_HPP_

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Relational/JavaUDF/AbstractJavaUDFOperator.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This operator evaluates a flat map expression defined as java function on input records.
 * Its state is managed inside a JavaUDFOperatorHandler.
 */
class FlatMapJavaUDF : public AbstractJavaUDFOperator {
  public:
    /**
     * @brief Creates a FlatMapJavaUDF operator
     * @param operatorHandlerIndex The index to a valid JavaUDFOperatorHandler
     * @param operatorInputSchema The input schema of the flat map operator.
     * @param operatorOutputSchema The output schema of the flat map operator.
     */
    FlatMapJavaUDF(uint64_t operatorHandlerIndex, SchemaPtr operatorInputSchema, SchemaPtr operatorOutputSchema);
    /**
     * Operator execution function
     * @param ctx operator context
     * @param record input record
     */
    void execute(ExecutionContext& ctx, Record& record) const override;
};

}// namespace NES::Runtime::Execution::Operators

#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JAVAUDF_FLATMAPJAVAUDF_HPP_
