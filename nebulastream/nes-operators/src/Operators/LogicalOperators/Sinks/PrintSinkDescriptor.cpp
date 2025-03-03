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

#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <string>

namespace NES {

PrintSinkDescriptor::PrintSinkDescriptor(uint64_t numberOfOrigins) : SinkDescriptor(numberOfOrigins) {}

SinkDescriptorPtr PrintSinkDescriptor::create(uint64_t numberOfOrigins) {
    return std::make_shared<PrintSinkDescriptor>(PrintSinkDescriptor(numberOfOrigins));
}

std::string PrintSinkDescriptor::toString() const { return "PrintSinkDescriptor()"; }

bool PrintSinkDescriptor::equal(SinkDescriptorPtr const& other) { return other->instanceOf<PrintSinkDescriptor>(); }

}// namespace NES
