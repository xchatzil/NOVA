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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WATERMARKS_WATERMARKSTRATEGYDESCRIPTOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WATERMARKS_WATERMARKSTRATEGYDESCRIPTOR_HPP_

#include <memory>

namespace NES {
class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

}// namespace NES

namespace NES::Windowing {

class WatermarkStrategyDescriptor;
using WatermarkStrategyDescriptorPtr = std::shared_ptr<WatermarkStrategyDescriptor>;

class WatermarkStrategyDescriptor : public std::enable_shared_from_this<WatermarkStrategyDescriptor> {
  public:
    WatermarkStrategyDescriptor();
    virtual ~WatermarkStrategyDescriptor() = default;
    virtual bool equal(WatermarkStrategyDescriptorPtr other) = 0;

    virtual std::string toString() = 0;

    /**
    * @brief Checks if the current node is of type WatermarkStrategyDescriptor
    * @tparam WatermarkStrategyType
    * @return bool true if node is of WatermarkStrategyDescriptor
    */
    template<class WatermarkStrategyType>
    bool instanceOf() const {
        if (dynamic_cast<WatermarkStrategyType*>(this)) {
            return true;
        }
        return false;
    };

    template<class WatermarkStrategyType>
    bool instanceOf() {
        if (dynamic_cast<WatermarkStrategyType*>(this)) {
            return true;
        }
        return false;
    };

    /**
    * @brief Dynamically casts the watermark strategy to a WatermarkStrategyType
    * @tparam WatermarkStrategyType
    * @return returns a shared pointer of the WatermarkStrategyType
    */
    template<class WatermarkStrategyType>
    std::shared_ptr<WatermarkStrategyType> as() const {
        if (instanceOf<const WatermarkStrategyType>()) {
            return std::dynamic_pointer_cast<const WatermarkStrategyType>(this->shared_from_this());
        }
        throw std::bad_cast();
    }
    template<class WatermarkStrategyType>
    std::shared_ptr<WatermarkStrategyType> as() {
        if (instanceOf<WatermarkStrategyType>()) {
            return std::dynamic_pointer_cast<WatermarkStrategyType>(this->shared_from_this());
        }
        throw std::bad_cast();
    }

    virtual bool inferStamp(SchemaPtr schema) = 0;
};
}// namespace NES::Windowing

#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WATERMARKS_WATERMARKSTRATEGYDESCRIPTOR_HPP_
