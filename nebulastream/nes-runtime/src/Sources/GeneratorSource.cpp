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

#include <Runtime/BufferManager.hpp>
#include <Sources/GeneratorSource.hpp>

#include <iostream>

namespace NES {

std::string GeneratorSource::toString() const {
    std::stringstream ss;
    ss << "GENERATOR_SOURCE(SCHEMA(" << schema->toString();
    ss << "), NUM_BUFFERS=" << this->numberOfBuffersToProduce << " interval=" << this->gatheringInterval.count() << "ms))";
    return ss.str();
}

SourceType GeneratorSource::getType() const { return SourceType::TEST_SOURCE; }// todo add generator source?

}// namespace NES
