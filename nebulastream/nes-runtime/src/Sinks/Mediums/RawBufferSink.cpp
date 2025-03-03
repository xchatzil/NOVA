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

#include <Exceptions/TaskExecutionException.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Mediums/RawBufferSink.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>
#include <filesystem>
#include <iostream>
#include <utility>

namespace NES {

SinkMediumTypes RawBufferSink::getSinkMediumType() { return SinkMediumTypes::FILE_SINK; }

RawBufferSink::RawBufferSink(Runtime::NodeEnginePtr nodeEngine,
                             uint32_t numOfProducers,
                             const std::string& filePath,
                             bool append,
                             SharedQueryId sharedQueryId,
                             DecomposedQueryId decomposedQueryId,
                             uint64_t numberOfOrigins)
    : SinkMedium(nullptr, std::move(nodeEngine), numOfProducers, sharedQueryId, decomposedQueryId, numberOfOrigins),
      filePath(filePath), append(append) {}

RawBufferSink::~RawBufferSink() {}

std::string RawBufferSink::toString() const {
    std::stringstream ss;
    ss << "RawBufferSink(";
    ss << ")";
    return ss.str();
}

void RawBufferSink::setup() {
    NES_DEBUG("Setting up raw buffer sink; filePath={}, append={}", filePath, append);
    // Remove an existing file unless the append mode is APPEND.
    if (!append) {
        if (std::filesystem::exists(filePath.c_str())) {
            std::error_code ec;
            if (!std::filesystem::remove(filePath.c_str(), ec)) {
                NES_ERROR("Could not remove existing output file: filePath={}, error={}", filePath, ec.message());
                isOpen = false;
                return;
            }
        }
    }

    // Open the file stream
    if (!outputFile.is_open()) {
        outputFile.open(filePath, std::ofstream::binary | std::ofstream::out);
    }
    isOpen = outputFile.is_open() && outputFile.good();
    if (!isOpen) {
        NES_ERROR("Could not open output file; filePath={}, is_open()={}, good={}",
                  filePath,
                  outputFile.is_open(),
                  outputFile.good());
    }
}

void RawBufferSink::shutdown() {
    NES_DEBUG("Closing file sink, filePath={}", filePath);
    outputFile.close();
}

bool RawBufferSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) {
    // Stop execution if the file could not be opened during setup.
    // This results in ExecutionResult::Error for the task.
    if (!isOpen) {
        NES_DEBUG("The output file could not be opened during setup of the file sink.");
        return false;
    }
    std::unique_lock lock(writeMutex);

    if (!inputBuffer) {
        NES_ERROR("Invalid input buffer");
        return false;
    }

    NES_DEBUG("Writing tuples to file sink; filePath={}", filePath);
    // 1. write buffer size
    auto size = inputBuffer.getBufferSize();
    outputFile.write(reinterpret_cast<char*>(&size), sizeof(uint64_t));
    // 2. write number of tuples in buffer
    auto numberOfTuples = inputBuffer.getNumberOfTuples();
    outputFile.write(reinterpret_cast<char*>((&numberOfTuples)), sizeof(uint64_t));
    // 3. write sequence number of buffer
    auto seqNumber = inputBuffer.getSequenceNumber();
    outputFile.write(reinterpret_cast<char*>((&seqNumber)), sizeof(uint64_t));
    // 4. write buffer content
    outputFile.write(reinterpret_cast<char*>(inputBuffer.getBuffer()), size);
    outputFile.flush();
    return true;
}

}// namespace NES
