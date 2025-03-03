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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_SENDINGPOLICY_SENDINGPOLICYLAZY_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_SENDINGPOLICY_SENDINGPOLICYLAZY_HPP_
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicy.hpp>

namespace NES::Statistic {

// defines for the sending policies. This way, we reduce the number of ()
#define SENDING_LAZY(StatisticDataCodec) SendingPolicyLazy::create(StatisticDataCodec)

/**
 * @brief Represents a sending policy, where a created statistic is send to the store, if it gets probed
 */
class SendingPolicyLazy : public SendingPolicy {
  public:
    /**
     * @brief Creates a string representation
     * @return std::string
     */
    [[nodiscard]] std::string toString() const override;

    /**
     * @brief Virtual destructor
     */
    ~SendingPolicyLazy() override = default;

    bool operator==(const SendingPolicy& rhs) const override;

    /**
     * @brief Creates a LAZY SendingPolicy
     * @return SendingPolicyPtr
     */
    static SendingPolicyPtr create(StatisticDataCodec sinkDataCodec);

  private:
    explicit SendingPolicyLazy(StatisticDataCodec sinkDataCodec);
};

}// namespace NES::Statistic

#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_SENDINGPOLICY_SENDINGPOLICYLAZY_HPP_
