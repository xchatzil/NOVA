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
#include <API/AttributeField.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJBuildSlicing.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJOperatorHandlerSlicing.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::Runtime::Execution::Operators {

uint64_t getNLJSliceStartProxy(void* ptrNljSlice) {
    NES_ASSERT2_FMT(ptrNljSlice != nullptr, "nlj slice pointer should not be null!");
    auto* nljSlice = static_cast<NLJSlice*>(ptrNljSlice);
    return nljSlice->getSliceStart();
}

uint64_t getNLJSliceEndProxy(void* ptrNljSlice) {
    NES_ASSERT2_FMT(ptrNljSlice != nullptr, "nlj slice pointer should not be null!");
    auto* nljSlice = static_cast<NLJSlice*>(ptrNljSlice);
    return nljSlice->getSliceEnd();
}

void* getCurrentWindowProxy(void* ptrOpHandler, uint64_t joinStrategyInt, uint64_t windowingStrategyInt) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = StreamJoinOperator::getSpecificOperatorHandler(ptrOpHandler, joinStrategyInt, windowingStrategyInt);
    return dynamic_cast<NLJOperatorHandlerSlicing*>(opHandler)->getCurrentSliceOrCreate();
}

void* getNLJSliceRefProxy(void* ptrOpHandler, uint64_t timestamp, uint64_t joinStrategyInt, uint64_t windowingStrategyInt) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = StreamJoinOperator::getSpecificOperatorHandler(ptrOpHandler, joinStrategyInt, windowingStrategyInt);
    return dynamic_cast<NLJOperatorHandlerSlicing*>(opHandler)->getSliceByTimestampOrCreateIt(timestamp).get();
}

void NLJBuildSlicing::execute(ExecutionContext& ctx, Record& record) const {
    // Get the local state
    auto localJoinState = dynamic_cast<LocalNestedLoopJoinState*>(ctx.getLocalState(this));
    auto operatorHandlerMemRef = localJoinState->joinOperatorHandler;
    Value<UInt64> timestampVal = timeFunction->getTs(ctx, record);

    if (!(localJoinState->sliceStart <= timestampVal && timestampVal < localJoinState->sliceEnd)) {
        // We have to get the slice for the current timestamp
        auto workerThreadId = ctx.getWorkerThreadId();
        updateLocalJoinState(localJoinState, operatorHandlerMemRef, timestampVal);
    }

    // Write record to the pagedVector
    auto nljPagedVectorMemRef = Nautilus::FunctionCall("getNLJPagedVectorProxy",
                                                       getNLJPagedVectorProxy,
                                                       localJoinState->sliceReference,
                                                       ctx.getWorkerThreadId(),
                                                       Value<UInt64>(to_underlying(joinBuildSide)));
    Nautilus::Interface::PagedVectorVarSizedRef pagedVectorVarSizedRef(nljPagedVectorMemRef, schema);
    pagedVectorVarSizedRef.writeRecord(record);
}

void NLJBuildSlicing::updateLocalJoinState(LocalNestedLoopJoinState* localJoinState,
                                           Value<Nautilus::MemRef>& operatorHandlerMemRef,
                                           Value<Nautilus::UInt64>& timestamp) const {
    NES_DEBUG("Updating LocalJoinState for timestamp {}!", timestamp->toString());

    // Retrieving the slice of the current watermark, as we expect that more tuples will be inserted into this slice
    localJoinState->sliceReference =
        Nautilus::FunctionCall("getNLJSliceRefProxy",
                               getNLJSliceRefProxy,
                               operatorHandlerMemRef,
                               timestamp,
                               Value<UInt64>(to_underlying<QueryCompilation::StreamJoinStrategy>(joinStrategy)),
                               Value<UInt64>(to_underlying<QueryCompilation::WindowingStrategy>(windowingStrategy)));
    localJoinState->sliceStart =
        Nautilus::FunctionCall("getNLJSliceStartProxy", getNLJSliceStartProxy, localJoinState->sliceReference);
    localJoinState->sliceEnd = Nautilus::FunctionCall("getNLJSliceEndProxy", getNLJSliceEndProxy, localJoinState->sliceReference);
}

void NLJBuildSlicing::open(ExecutionContext& ctx, RecordBuffer&) const {
    auto opHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    auto workerThreadId = ctx.getWorkerThreadId();
    auto sliceReference =
        Nautilus::FunctionCall("getCurrentWindowProxy",
                               getCurrentWindowProxy,
                               opHandlerMemRef,
                               Value<UInt64>(to_underlying<QueryCompilation::StreamJoinStrategy>(joinStrategy)),
                               Value<UInt64>(to_underlying<QueryCompilation::WindowingStrategy>(windowingStrategy)));
    auto nljPagedVectorMemRef = Nautilus::FunctionCall("getNLJPagedVectorProxy",
                                                       getNLJPagedVectorProxy,
                                                       sliceReference,
                                                       workerThreadId,
                                                       Value<UInt64>(to_underlying(joinBuildSide)));
    auto pagedVectorVarSizedRef = Nautilus::Interface::PagedVectorVarSizedRef(nljPagedVectorMemRef, schema);
    auto localJoinState = std::make_unique<LocalNestedLoopJoinState>(opHandlerMemRef, sliceReference, pagedVectorVarSizedRef);

    // Getting the current slice start and end
    localJoinState->sliceStart =
        Nautilus::FunctionCall("getNLJSliceStartProxy", getNLJSliceStartProxy, localJoinState->sliceReference);
    localJoinState->sliceEnd = Nautilus::FunctionCall("getNLJSliceEndProxy", getNLJSliceEndProxy, localJoinState->sliceReference);

    // Storing the local state
    ctx.setLocalOperatorState(this, std::move(localJoinState));
}

NLJBuildSlicing::NLJBuildSlicing(const uint64_t operatorHandlerIndex,
                                 const SchemaPtr& schema,
                                 const QueryCompilation::JoinBuildSideType joinBuildSide,
                                 const uint64_t entrySize,
                                 TimeFunctionPtr timeFunction,
                                 QueryCompilation::StreamJoinStrategy joinStrategy)
    : StreamJoinOperator(joinStrategy, QueryCompilation::WindowingStrategy::SLICING), StreamJoinBuild(operatorHandlerIndex,
                                                                                                      schema,
                                                                                                      joinBuildSide,
                                                                                                      entrySize,
                                                                                                      std::move(timeFunction),
                                                                                                      joinStrategy,
                                                                                                      windowingStrategy) {}
}// namespace NES::Runtime::Execution::Operators
