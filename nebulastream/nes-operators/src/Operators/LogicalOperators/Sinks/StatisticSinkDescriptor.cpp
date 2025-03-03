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

#include <Operators/LogicalOperators/Sinks/StatisticSinkDescriptor.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <fmt/format.h>

namespace NES::Statistic {

std::string StatisticSinkDescriptor::toString() const {
    return fmt::format("StatisticSinkDescriptor({})", magic_enum::enum_name(sinkFormatType));
}

SinkDescriptorPtr StatisticSinkDescriptor::create(StatisticSynopsisType sinkFormatType,
                                                  StatisticDataCodec sinkDataCodec,
                                                  uint64_t numberOfOrigins) {
    return std::make_shared<StatisticSinkDescriptor>(StatisticSinkDescriptor(sinkFormatType, sinkDataCodec, numberOfOrigins));
}

bool StatisticSinkDescriptor::equal(const SinkDescriptorPtr& other) {
    if (other->instanceOf<StatisticSinkDescriptor>()) {
        const auto otherStatisticSinkDescriptor = other->as<StatisticSinkDescriptor>();
        return numberOfOrigins == otherStatisticSinkDescriptor->numberOfOrigins
            && sinkFormatType == otherStatisticSinkDescriptor->sinkFormatType
            && sinkDataCodec == otherStatisticSinkDescriptor->sinkDataCodec;
    }
    return false;
}

StatisticSynopsisType StatisticSinkDescriptor::getSinkFormatType() const { return sinkFormatType; }
StatisticDataCodec StatisticSinkDescriptor::getSinkDataCodec() const { return sinkDataCodec; }

StatisticSinkDescriptor::StatisticSinkDescriptor(StatisticSynopsisType sinkFormatType,
                                                 StatisticDataCodec sinkDataCodec,
                                                 uint64_t numberOfOrigins)
    : SinkDescriptor(numberOfOrigins), sinkFormatType(sinkFormatType), sinkDataCodec(sinkDataCodec) {}

}// namespace NES::Statistic
