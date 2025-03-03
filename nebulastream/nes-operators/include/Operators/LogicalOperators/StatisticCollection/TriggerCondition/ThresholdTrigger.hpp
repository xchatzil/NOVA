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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_TRIGGERCONDITION_THRESHOLDTRIGGER_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_TRIGGERCONDITION_THRESHOLDTRIGGER_HPP_

#include <Operators/LogicalOperators/StatisticCollection/TriggerCondition/TriggerCondition.hpp>

namespace NES::Statistic {

/**
 * @brief Checks if the latest statistic is above a threshold
 */
template<typename T>
class ThresholdTrigger : public TriggerCondition {
  public:
    /**
     * @brief Creates a string representation
     * @return std::string
     */
    [[nodiscard]] std::string toString() const override;

  private:
    const T threshold;
};

}// namespace NES::Statistic

#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_TRIGGERCONDITION_THRESHOLDTRIGGER_HPP_
