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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_AGGREGATION_QUANTILEESTIMATIONAGGREGATION_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_AGGREGATION_QUANTILEESTIMATIONAGGREGATION_HPP_

#include <Execution/Aggregation/AggregationFunction.hpp>

/**
 * This class uses the Nautilus Aggregation Interface for the Tdigest algorithms.
 * The current implementation derives the median, by fixing issue #3889 this class will be able to estimate any quantile value (0-100).
 * The class creates an instance of the tdigest and adds continuously new input tuples (lift).
 * In contrast to standard aggregation function, we once create the instance of the tdigest and update it with every new tuple
 * instead of deriving a new value. The class also allows to combine multiple
 * instances of the tdigest with combine and to derive the final quantile value estimation with lower.
 * For the algorithm itself we utilize the implementation @link: https://github.com/SpirentOrion/digestible.
 */
namespace NES::Runtime::Execution::Aggregation {
class QuantileEstimationAggregation : public AggregationFunction {

  public:
    QuantileEstimationAggregation(const PhysicalTypePtr& inputType,
                                  const PhysicalTypePtr& finalType,
                                  const Expressions::ExpressionPtr& inputExpression,
                                  const Nautilus::Record::RecordFieldIdentifier& resultFieldIdentifier);

    void lift(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Record& record) override;
    void combine(Nautilus::Value<Nautilus::MemRef> memref1, Nautilus::Value<Nautilus::MemRef> memref2) override;
    void lower(Nautilus::Value<Nautilus::MemRef> memref, Nautilus::Record& record) override;
    void reset(Nautilus::Value<Nautilus::MemRef> memref) override;
    uint64_t getSize() override;
};
}// namespace NES::Runtime::Execution::Aggregation

#endif// NES_EXECUTION_INCLUDE_EXECUTION_AGGREGATION_QUANTILEESTIMATIONAGGREGATION_HPP_
