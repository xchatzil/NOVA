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

#ifndef NES_COORDINATOR_TESTS_INCLUDE_UTIL_DUMMYSINK_HPP_
#define NES_COORDINATOR_TESTS_INCLUDE_UTIL_DUMMYSINK_HPP_
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <memory>
#include <string>
namespace NES {
class DummySink : public SinkDescriptor {
  public:
    static SinkDescriptorPtr create() { return std::make_shared<DummySink>(); }
    DummySink() = default;
    ~DummySink() override = default;
    [[nodiscard]] std::string toString() const override { return std::string(); }
    [[nodiscard]] bool equal(SinkDescriptorPtr const&) override { return false; }
};
}// namespace NES
#endif// NES_COORDINATOR_TESTS_INCLUDE_UTIL_DUMMYSINK_HPP_
