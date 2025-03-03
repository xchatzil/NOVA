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

#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandlerSlicing.hpp>

namespace NES::Runtime::Execution::Operators {

StreamSlicePtr StreamJoinOperatorHandlerSlicing::getSliceByTimestampOrCreateIt(uint64_t timestamp) {
    auto [slicesWriteLocked, windowToSlicesLocked] = folly::acquireLocked(slices, windowToSlices);

    // Checking, if we maybe already have this slice
    auto sliceStart = sliceAssigner.getSliceStartTs(timestamp);
    auto sliceEnd = sliceAssigner.getSliceEndTs(timestamp);
    auto sliceId = StreamSlice::getSliceIdentifier(sliceStart, sliceEnd);
    auto slice = getSliceBySliceIdentifier(slicesWriteLocked, sliceId);
    if (slice.has_value()) {
        return slice.value();
    }

    // No slice was found for the timestamp
    NES_TRACE("Creating slice for slice start={} and end={} for ts={}", sliceStart, sliceEnd, timestamp);
    auto newSlice = createNewSlice(sliceStart, sliceEnd);
    slicesWriteLocked->emplace_back(newSlice);

    // For all possible slices in their respective windows, reset the state
    for (auto windowInfo : getAllWindowsForSlice(*newSlice)) {
        NES_TRACE("reset the state for window {}", windowInfo.toString());
        auto& window = (*windowToSlicesLocked)[windowInfo];
        window.windowState = WindowInfoState::BOTH_SIDES_FILLING;
        window.slices.emplace_back(newSlice);
        NES_TRACE("Added slice {} to window {}", newSlice->toString(), windowInfo.toString());
    }
    return newSlice;
}

StreamSlice* StreamJoinOperatorHandlerSlicing::getCurrentSliceOrCreate() {
    if (slices.rlock()->empty()) {
        return StreamJoinOperatorHandlerSlicing::getSliceByTimestampOrCreateIt(0).get();
    }
    return slices.rlock()->back().get();
}

std::vector<WindowInfo> StreamJoinOperatorHandlerSlicing::getAllWindowsForSlice(StreamSlice& slice) {
    std::vector<WindowInfo> allWindows;

    const auto sliceStart = slice.getSliceStart();
    const auto sliceEnd = slice.getSliceEnd();
    /**
     * To get all windows, we start with the window (firstWindow) that the current slice is the last slice in it.
     * Or in other words: The sliceEnd is equal to the windowEnd.
     * We then add all windows with a slide until we reach the window (lastWindow) that that current slice is the first slice.
     * Or in other words: The sliceStart is equal to the windowStart;
     */
    const auto firstWindowEnd = sliceEnd;
    const auto lastWindowEnd = sliceStart + windowSize;
    for (auto curWindowEnd = firstWindowEnd; curWindowEnd <= lastWindowEnd; curWindowEnd += windowSlide) {
        // For now, we expect the windowEnd to be the windowId
        allWindows.emplace_back(curWindowEnd - windowSize, curWindowEnd);
    }

    return allWindows;
}
}// namespace NES::Runtime::Execution::Operators
