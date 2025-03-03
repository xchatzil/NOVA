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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALLIMITOPERATOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALLIMITOPERATOR_HPP_

#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>

namespace NES {

/**
 * @brief Limit operator
 */
class LogicalLimitOperator : public LogicalUnaryOperator {
  public:
    explicit LogicalLimitOperator(uint64_t limit, OperatorId id);
    ~LogicalLimitOperator() override = default;

    /**
   * @brief get the limit count.
   * @return limit
   */
    uint64_t getLimit() const;

    /**
     * @brief check if two operators have the same limit predicate.
     * @param rhs the operator to compare
     * @return bool true if they are the same otherwise false
     */
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;
    std::string toString() const override;

    /**
    * @brief Infers the input and output schema of this operator depending on its child.
    * @throws Exception the predicate expression has to return a boolean.
    * @param typeInferencePhaseContext needed for stamp inferring
    * @return true if schema was correctly inferred
    */
    bool inferSchema() override;
    OperatorPtr copy() override;
    void inferStringSignature() override;

  private:
    uint64_t limit;
};

}// namespace NES
#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALLIMITOPERATOR_HPP_
