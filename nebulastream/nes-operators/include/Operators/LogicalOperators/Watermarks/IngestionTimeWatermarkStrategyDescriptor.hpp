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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WATERMARKS_INGESTIONTIMEWATERMARKSTRATEGYDESCRIPTOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WATERMARKS_INGESTIONTIMEWATERMARKSTRATEGYDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Watermarks/WatermarkStrategyDescriptor.hpp>

namespace NES::Windowing {

class IngestionTimeWatermarkStrategyDescriptor;
using IngestionTimeWatermarkStrategyDescriptorPtr = std::shared_ptr<IngestionTimeWatermarkStrategyDescriptor>;

class IngestionTimeWatermarkStrategyDescriptor : public WatermarkStrategyDescriptor {
  public:
    static WatermarkStrategyDescriptorPtr create();

    bool equal(WatermarkStrategyDescriptorPtr other) override;
    std::string toString() override;

    /**
     * @brief Infer schema of watermark strategy descriptor
     * @param schema : the schema to be used for inferring the types
     * @return true if success else false
     */
    bool inferStamp(SchemaPtr schema) override;

  private:
    explicit IngestionTimeWatermarkStrategyDescriptor();
};

}// namespace NES::Windowing

#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WATERMARKS_INGESTIONTIMEWATERMARKSTRATEGYDESCRIPTOR_HPP_
