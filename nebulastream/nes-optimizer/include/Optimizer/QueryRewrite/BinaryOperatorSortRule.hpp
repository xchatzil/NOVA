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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_BINARYOPERATORSORTRULE_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_BINARYOPERATORSORTRULE_HPP_

#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>

namespace NES {

class BinaryOperator;
using BinaryOperatorPtr = std::shared_ptr<BinaryOperator>;

}// namespace NES

namespace NES::Optimizer {

class BinaryOperatorSortRule;
using BinaryOperatorSortRulePtr = std::shared_ptr<BinaryOperatorSortRule>;

/**
 * @brief This rule sorts children of a binary operators (Join and Union) by qualifier name.
 *
 * Example:
 *     1. Query::from("car").unionWith(Query::from("truck")).sink(); =>  Query::from("truck").unionWith(Query::from("car")).sink();
 *
 *     2. Query::from("truck").unionWith(Query::from("car")).sink(); =>  Query::from("truck").unionWith(Query::from("car")).sink();
 *
 *     3. Query::from("car").joinWith(Query::from("truck")).sink(); =>  Query::from("truck").joinWith(Query::from("car")).sink();
 */
class BinaryOperatorSortRule : public BaseRewriteRule {

  public:
    static BinaryOperatorSortRulePtr create();
    virtual ~BinaryOperatorSortRule() = default;

    QueryPlanPtr apply(QueryPlanPtr queryPlanPtr) override;

  private:
    /**
     * @brief This method takes input as a binary operator and sort the children alphabetically based on source qualifier name
     * @param binaryOperator : the input binary operator
     */
    static void sortChildren(const BinaryOperatorPtr& binaryOperator);

    BinaryOperatorSortRule();
};
}// namespace NES::Optimizer

#endif// NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_BINARYOPERATORSORTRULE_HPP_
