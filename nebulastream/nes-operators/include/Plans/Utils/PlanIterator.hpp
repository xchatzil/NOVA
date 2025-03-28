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

#ifndef NES_OPERATORS_INCLUDE_PLANS_UTILS_PLANITERATOR_HPP_
#define NES_OPERATORS_INCLUDE_PLANS_UTILS_PLANITERATOR_HPP_

#include <iterator>
#include <stack>

namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class DecomposedQueryPlan;
using DecomposedQueryPlanPtr = std::shared_ptr<DecomposedQueryPlan>;

/**
 * @brief Iterator for query plans, which correctly handles multiple sources and sinks.
 * The iterator visits each operator exactly one time in the following order:
 * top-to-bottom and left-to-right
 *
 * Example Query Plan:
 *
 * -- Sink 1 ---                            --- Source 1 ---
 *              \                         /
 *                --- Filter --- Join ---
 *              /                         \
 * -- Sink 2 ---                            --- Source 2 ---
 *
 * Iteration order:
 * #1 - Sink 1
 * #2 - Sink 2
 * #3 - Filter
 * #4 - Join
 * #5 - Source 1
 * #6 - Source 2
 */
class PlanIterator {
  public:
    explicit PlanIterator(QueryPlanPtr queryPlan);

    explicit PlanIterator(DecomposedQueryPlanPtr decomposedQueryPlan);

    class iterator : public std::iterator<std::forward_iterator_tag, NodePtr, NodePtr, NodePtr*, NodePtr&> {
        friend class PlanIterator;

      public:
        /**
         * @brief Moves the iterator to the next node.
         * If we reach the end of the iterator we will ignore this operation.
         * @return iterator
         */
        iterator& operator++();

        /**
         * @brief Checks if the iterators are not at the same position
         */
        bool operator!=(const iterator& other) const;

        /**
         * @brief Gets the node at the current iterator position.
         * @return
         */
        NodePtr operator*();

      private:
        explicit iterator(const std::vector<OperatorPtr>& rootOperators);
        explicit iterator();
        std::stack<NodePtr> workStack;
    };

    /**
     * @brief Starts a new iterator at the start node, which is always a sink.
     * @return iterator
     */
    iterator begin();

    /**
    * @brief The end of this iterator has an empty work stack.
    * @return iterator
    */
    static iterator end();

    /**
     * @brief Return a snapshot of the iterator.
     * @return vector<NodePtr> nodes
     */
    std::vector<NodePtr> snapshot();

  private:
    std::vector<OperatorPtr> rootOperators;
};

}// namespace NES

#endif// NES_OPERATORS_INCLUDE_PLANS_UTILS_PLANITERATOR_HPP_
