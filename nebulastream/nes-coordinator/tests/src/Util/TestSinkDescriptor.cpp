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
#include <Util/TestSinkDescriptor.hpp>

namespace NES::TestUtils {

TestSinkDescriptor::TestSinkDescriptor(DataSinkPtr dataSink) : sink(std::move(dataSink)) {}

DataSinkPtr TestSinkDescriptor::getSink() { return sink; }

std::string TestSinkDescriptor::toString() const { return std::string(); }

bool TestSinkDescriptor::equal(const SinkDescriptorPtr&) { return false; }

}// namespace NES::TestUtils
