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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLER_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLER_HPP_
#include <API/Schema.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Execution/Operators/Streaming/SliceAssigner.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Util/Common.hpp>
#include <folly/Synchronized.h>
#include <list>

namespace NES::Runtime::Execution::Operators {

class StreamJoinOperatorHandler;
using StreamJoinOperatorHandlerPtr = std::shared_ptr<StreamJoinOperatorHandler>;

using WLockedSlices = folly::Synchronized<std::list<StreamSlicePtr>>::WLockedPtr;
using RLockedSlices = folly::Synchronized<std::list<StreamSlicePtr>>::RLockedPtr;

/**
 * @brief This operator is the general join operator handler and implements the JoinOperatorHandlerInterface. It is expected that
 * all StreamJoinOperatorHandlers inherit from this
 */
class StreamJoinOperatorHandler : public virtual OperatorHandler {
  public:
    /**
     * @brief Constructor for a StreamJoinOperatorHandler
     * @param inputOrigins
     * @param outputOriginId
     * @param windowSize
     * @param windowSlide
     * @param sizeOfRecordLeft
     * @param sizeOfRecordRight
     */
    StreamJoinOperatorHandler(const std::vector<OriginId>& inputOrigins,
                              const OriginId outputOriginId,
                              const uint64_t windowSize,
                              const uint64_t windowSlide,
                              const SchemaPtr& leftSchema,
                              const SchemaPtr& rightSchema);

    ~StreamJoinOperatorHandler() override = default;

    void start(PipelineExecutionContextPtr pipelineExecutionContext, uint32_t localStateVariableId) override;
    void stop(QueryTerminationType terminationType, PipelineExecutionContextPtr pipelineExecutionContext) override;

    /**
     * @brief Retrieve the state as a vector of tuple buffers
     * Format of buffers looks like:
     * start buffers contain metadata in format:
     * -----------------------------------------
     * number of metadata buffers | number of slices (n) | number of buffers in 1st slice (m_0) | ... | number of slices in n-th slice (m_n)
     * uint64_t | uint64_t | uint64_t | ... | uint64_t
     * -----------------------------------------
     * all other buffers are: 1st buffer of 1st slice | .... | m_0 buffer of 1 slice | ... | 1 buffer of n-th slice | m_n buffer of n-th slice
     * @param startTS as a left border of slices
     * @param stopTS as a right border of slice
     * @return vector of tuple buffers
     */
    std::vector<Runtime::TupleBuffer> getStateToMigrate(uint64_t startTS, uint64_t stopTS) override;

    /**
     * @brief Retrieve window info as a vector of tuple buffers
     * Format of data buffers:
     * -----------------------------------------
     * number of windows (n) | start ts of 0 window |  end ts of 0 window | windowInfoState of 0 window | ... | start ts of n-th window |  end ts of n-th window | windowInfoState of n-th window
     * uint64_t | uint64_t | uint64_t | uint64_t | ....
     * -----------------------------------------
     * @return vector of tuple buffers
     */
    std::vector<Runtime::TupleBuffer> getWindowInfoToMigrate();

    /**
     * @brief Restores window info from vector of tuple buffers
     * Expected format of data buffers:
     * -----------------------------------------
     * number of windows (n) | start ts of 0 window |  end ts of 0 window | windowInfoState of 0 window | ... | start ts of n-th window |  end ts of n-th window | windowInfoState of n-th window
     * uint64_t | uint64_t | uint64_t | uint64_t | ....
     * -----------------------------------------
     * @param buffers with the data for recreation
     */
    void restoreWindowInfo(std::vector<Runtime::TupleBuffer>& buffers);

    /**
     * @brief Restores the state from vector of tuple buffers
     * Expected format of buffers:
     * start buffers contain metadata in format:
     * -----------------------------------------
     * number of slices (n) | number of buffers in 1st slice (m_0) | ... | number of slices in n-th slice (m_n)
     * uint64_t | uint64_t | uint64_t | ... | uint64_t
     *-----------------------------------------
     * all other buffers are: 1st buffer of 1st slice | .... | m_0 buffer of 1 slice | ... | 1 buffer of n-th slice | m_n buffer of n-th slice
     * @param buffers with the state
     */
    void restoreState(std::vector<Runtime::TupleBuffer>& buffers) override;

    /**
     * @brief Retrieves buffers from the file and restores the state
     * @param stream with the state data
     */
    void restoreStateFromFile(std::ifstream& stream) override;

    /**
     * @brief Retrieves the slice/window by a slice/window identifier. If no slice/window exists for the windowIdentifier,
     * the optional return value is of nullopt.
     * @param sliceIdentifier
     * @return Optional
     */
    std::optional<StreamSlicePtr> getSliceBySliceIdentifier(uint64_t sliceIdentifier);
    std::optional<StreamSlicePtr> getSliceBySliceIdentifier(const RLockedSlices& slicesLocked, uint64_t sliceIdentifier);
    std::optional<StreamSlicePtr> getSliceBySliceIdentifier(const WLockedSlices& slicesLocked, uint64_t sliceIdentifier);

    /**
     * @brief Triggers all slices/windows that have not been already emitted to the probe
     */
    void triggerAllSlices(PipelineExecutionContext* pipelineCtx);

    /**
     * @brief Deletes all slices/windows
     */
    void deleteAllSlices();

    /**
     * @brief Triggers windows that are ready. This method updates the watermarkProcessor and should be thread-safe
     * @param bufferMetaData
     */
    void checkAndTriggerWindows(const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx);

    /**
     * @brief Updates the corresponding watermark processor and then deletes all slices/windows that are not valid anymore.
     * @param bufferMetaData
     */
    void deleteSlices(const BufferMetaData& bufferMetaData);

    /**
     * @brief Retrieves all window identifiers that correspond to this slice. For bucketing, this will just return one item
     * @param slice
     * @return Vector<WindowInfo>
     */
    virtual std::vector<WindowInfo> getAllWindowsForSlice(StreamSlice& slice) = 0;

    /**
     * @brief Get the number of current active slices/windows
     * @return uint64_t
     */
    uint64_t getNumberOfSlices();

    /**
     * @brief Returns the number of tuples in the slice/window for the given sliceIdentifier
     * @param sliceIdentifier
     * @param buildSide
     * @return number of tuples as uint64_t or -1 if no slice can be found for the sliceIdentifier
     */
    uint64_t getNumberOfTuplesInSlice(uint64_t sliceIdentifier, QueryCompilation::JoinBuildSideType buildSide);

    /**
     * @brief Creates a new slice/window for the given start and end
     * @param sliceStart
     * @param sliceEnd
     * @return StreamSlicePtr
     */
    virtual StreamSlicePtr createNewSlice(uint64_t sliceStart, uint64_t sliceEnd) = 0;

    /**
     * @brief Emits the left and right slice to the probe
     * @param sliceLeft
     * @param sliceRight
     * @param windowInfo
     * @param pipelineCtx
     */
    virtual void emitSliceIdsToProbe(StreamSlice& sliceLeft,
                                     StreamSlice& sliceRight,
                                     const WindowInfo& windowInfo,
                                     PipelineExecutionContext* pipelineCtx) = 0;

    /**
     * @brief Returns the output origin id that this handler is responsible for
     * @return OriginId
     */
    [[nodiscard]] OriginId getOutputOriginId() const;

    /**
     * @brief Returns the next sequence number for the operator that this operator handler is responsible for
     * @return uint64_t
     */
    uint64_t getNextSequenceNumber();

    /**
     * @brief Sets the number of worker threads for this operator handler
     * @param numberOfWorkerThreads
     */
    virtual void setNumberOfWorkerThreads(uint64_t numberOfWorkerThreads);

    /**
     * @brief update the watermark for a particular worker
     * @param watermark
     * @param workerThreadId
     */
    void updateWatermarkForWorker(uint64_t watermark, WorkerThreadId workerThreadId);

    /**
     * @brief Get the minimal watermark among all worker
     * @return uint64_t
     */
    uint64_t getMinWatermarkForWorker();

    /**
     * @brief Returns the window slide
     * @return uint64_t
     */
    uint64_t getWindowSlide() const;

    /**
     * @brief Returns the window size
     * @return uint64_t
     */
    uint64_t getWindowSize() const;

    void setBufferManager(const BufferManagerPtr& bufManager);

  private:
    /**
     * Deserialize slice from span of buffers, which is join specific and is implemented in sub-classes
     * @param buffers as a span
     * @return recreated StreamSlicePtr
     */
    virtual StreamSlicePtr deserializeSlice(std::span<const Runtime::TupleBuffer> buffers) = 0;

  protected:
    uint64_t numberOfWorkerThreads = 1;
    folly::Synchronized<std::list<StreamSlicePtr>> slices;
    uint64_t lastMigratedSeqNumber = 0;
    SliceAssigner sliceAssigner;
    uint64_t windowSize;
    uint64_t windowSlide;
    folly::Synchronized<std::map<WindowInfo, SlicesAndState>> windowToSlices;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessorBuild;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessorProbe;
    std::unordered_map<WorkerThreadId, uint64_t> workerThreadIdToWatermarkMap;
    const OriginId outputOriginId;
    std::atomic<uint64_t> sequenceNumber;
    std::atomic<bool> alreadySetup{false};
    // TODO with issue #4517 we can remove the sizes
    size_t sizeOfRecordLeft;
    size_t sizeOfRecordRight;
    SchemaPtr leftSchema;
    SchemaPtr rightSchema;
    BufferManagerPtr bufferManager;
};
}// namespace NES::Runtime::Execution::Operators

#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINOPERATORHANDLER_HPP_
