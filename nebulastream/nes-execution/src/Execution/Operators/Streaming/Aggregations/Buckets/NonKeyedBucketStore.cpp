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

#include <Execution/Operators/Streaming/Aggregations/Buckets/NonKeyedBucketStore.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <memory>

namespace NES::Runtime::Execution::Operators {

NonKeyedBucketStore::NonKeyedBucketStore(uint64_t entrySize,
                                         uint64_t windowSize,
                                         uint64_t windowSlide,
                                         const std::unique_ptr<State>& defaultState)
    : BucketStore<NonKeyedSlice>(windowSize, windowSlide), entrySize(entrySize), defaultState(defaultState) {}

GlobalSlicePtr NonKeyedBucketStore::allocateNewSlice(uint64_t startTs, uint64_t endTs) {
    NES_DEBUG("Allocate Slices {}-{}", startTs, endTs);
    return std::make_unique<NonKeyedSlice>(entrySize, startTs, endTs, defaultState);
}

}// namespace NES::Runtime::Execution::Operators
