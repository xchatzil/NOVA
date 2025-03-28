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

#ifndef NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_ARITHMETICALEXPRESSIONS_ARITHMETICALEXPRESSIONNODE_HPP_
#define NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_ARITHMETICALEXPRESSIONS_ARITHMETICALEXPRESSIONNODE_HPP_

namespace NES {
/**
 * @brief This class just indicates that a node is an arithmetical expression.
 */
class ArithmeticalExpressionNode {
  protected:
    ArithmeticalExpressionNode() = default;
    virtual ~ArithmeticalExpressionNode() noexcept = default;
};

}// namespace NES

#endif// NES_EXPRESSIONS_INCLUDE_EXPRESSIONS_ARITHMETICALEXPRESSIONS_ARITHMETICALEXPRESSIONNODE_HPP_
