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

#ifndef NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICINTERFACE_HPP_
#define NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICINTERFACE_HPP_

#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicy.hpp>
#include <Operators/LogicalOperators/StatisticCollection/TriggerCondition/TriggerCondition.hpp>
#include <Operators/LogicalOperators/Windows/WindowingForwardRefs.hpp>
#include <StatisticCollection/Characteristic/Characteristic.hpp>
#include <Statistics/StatisticKey.hpp>
#include <Statistics/StatisticValue.hpp>
#include <functional>

namespace NES::Statistic {

class StatisticInterface {
  public:
    /**
     * @brief Creates a request to track a specific statistic characteristic, e.g., Cardinality over car_1 in a
     * tumbling window fashion of 5 minutes
     * @param characteristic
     * @param window
     * @param triggerCondition
     * @param sendingPolicy
     * @param callBack
     */
    virtual std::vector<StatisticKey> trackStatistic(const CharacteristicPtr& characteristic,
                                                     const Windowing::WindowTypePtr& window,
                                                     const TriggerConditionPtr& triggerCondition,
                                                     const SendingPolicyPtr& sendingPolicy,
                                                     std::function<void(CharacteristicPtr)>&& callBack) = 0;

    /**
     * @brief Creates a request to probe a specific statistic and returns the statistic in a ProbeResult
     * @param statisticKey
     * @param startTs
     * @param endTs
     * @param granularity
     * @param probeExpression
     * @param estimationAllowed
     * @param aggFunction
     * @return ProbeResult<> containing at least one StatisticValue
     */
    virtual ProbeResult<> probeStatistic(const StatisticKey& statisticKey,
                                         const Windowing::TimeMeasure& startTs,
                                         const Windowing::TimeMeasure& endTs,
                                         const Windowing::TimeMeasure& granularity,
                                         const ProbeExpression& probeExpression,
                                         const bool& estimationAllowed,
                                         std::function<ProbeResult<>(ProbeResult<>)>&& aggFunction) = 0;

    /**
     * @brief Virtual destructor
     */
    virtual ~StatisticInterface() = default;
};

}// namespace NES::Statistic

#endif// NES_COORDINATOR_INCLUDE_STATISTICCOLLECTION_STATISTICINTERFACE_HPP_
