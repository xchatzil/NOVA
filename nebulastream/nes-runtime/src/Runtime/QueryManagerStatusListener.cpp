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

#include <Network/NetworkSink.hpp>
#include <Network/NetworkSource.hpp>
#include <Runtime/AsyncTaskExecutor.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/HardwareManager.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/ThreadPool.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>
#include <memory>
#include <variant>

namespace NES::Runtime {
void AbstractQueryManager::notifyQueryStatusChange(const Execution::ExecutableQueryPlanPtr& qep,
                                                   Execution::ExecutableQueryPlanStatus status) {
    NES_ASSERT(qep, "Invalid query plan object");
    if (status == Execution::ExecutableQueryPlanStatus::Finished) {
        for (const auto& source : qep->getSources()) {
            if (!std::dynamic_pointer_cast<Network::NetworkSource>(source)) {
                NES_ASSERT2_FMT(source->stop(Runtime::QueryTerminationType::Graceful),
                                "Cannot cleanup source " << source->getOperatorId());// just a clean-up op
            }
        }
        addReconfigurationMessage(qep->getSharedQueryId(),
                                  qep->getDecomposedQueryId(),
                                  ReconfigurationMessage(qep->getSharedQueryId(),
                                                         qep->getDecomposedQueryId(),
                                                         ReconfigurationType::Destroy,
                                                         inherited1::shared_from_this()),
                                  false);

        queryStatusListener->notifyQueryStatusChange(qep->getSharedQueryId(),
                                                     qep->getDecomposedQueryId(),
                                                     Execution::ExecutableQueryPlanStatus::Finished);

    } else if (status == Execution::ExecutableQueryPlanStatus::ErrorState) {
        addReconfigurationMessage(qep->getSharedQueryId(),
                                  qep->getDecomposedQueryId(),
                                  ReconfigurationMessage(qep->getSharedQueryId(),
                                                         qep->getDecomposedQueryId(),
                                                         ReconfigurationType::Destroy,
                                                         inherited1::shared_from_this()),
                                  false);

        queryStatusListener->notifyQueryStatusChange(qep->getSharedQueryId(),
                                                     qep->getDecomposedQueryId(),
                                                     Execution::ExecutableQueryPlanStatus::ErrorState);
    }
}

void AbstractQueryManager::notifySourceFailure(DataSourcePtr failedSource, const std::string reason) {
    NES_DEBUG("notifySourceFailure called for query id= {}  reason= {}", failedSource->getOperatorId(), reason);
    std::vector<Execution::ExecutableQueryPlanPtr> plansToFail;
    {
        std::unique_lock lock(queryMutex);
        plansToFail = sourceToQEPMapping[failedSource->getOperatorId()];
    }
    // we cant fail a query from a source because failing a query eventually calls stop on the failed query
    // this means we are going to call join on the source thread
    // however, notifySourceFailure may be called from the source thread itself, thus, resulting in a deadlock
    for (auto qepToFail : plansToFail) {
        auto future = asyncTaskExecutor->runAsync([this,
                                                   reason,
                                                   qepToFail = std::move(qepToFail)]() -> Execution::ExecutableQueryPlanPtr {
            NES_DEBUG("Going to fail query id={} subplan={}",
                      qepToFail->getDecomposedQueryId(),
                      qepToFail->getDecomposedQueryId());
            if (failExecutableQueryPlan(qepToFail)) {
                NES_DEBUG("Failed query id= {} subplan={}", qepToFail->getDecomposedQueryId(), qepToFail->getDecomposedQueryId());
                queryStatusListener->notifyQueryFailure(qepToFail->getSharedQueryId(), qepToFail->getDecomposedQueryId(), reason);
                return qepToFail;
            }
            return nullptr;
        });
    }
}

void AbstractQueryManager::notifyTaskFailure(Execution::SuccessorExecutablePipeline pipelineOrSink,
                                             const std::string& errorMessage) {

    DecomposedQueryId planId = INVALID_DECOMPOSED_QUERY_PLAN_ID;
    Execution::ExecutableQueryPlanPtr qepToFail;
    if (auto* pipe = std::get_if<Execution::ExecutablePipelinePtr>(&pipelineOrSink)) {
        planId = (*pipe)->getDecomposedQueryId();
    } else if (auto* sink = std::get_if<DataSinkPtr>(&pipelineOrSink)) {
        planId = (*sink)->getParentPlanId();
    }
    {
        std::unique_lock lock(queryMutex);
        if (auto it = runningQEPs.find(planId); it != runningQEPs.end()) {
            qepToFail = it->second;
        } else {
            NES_WARNING("Cannot fail non existing sub query plan: {}", planId);
            return;
        }
    }
    auto future = asyncTaskExecutor->runAsync(
        [this, errorMessage](Execution::ExecutableQueryPlanPtr qepToFail) -> Execution::ExecutableQueryPlanPtr {
            NES_DEBUG("Going to fail query id= {} subplan={}",
                      qepToFail->getDecomposedQueryId(),
                      qepToFail->getDecomposedQueryId());
            if (failExecutableQueryPlan(qepToFail)) {
                NES_DEBUG("Failed query id= {}  subplan= {}",
                          qepToFail->getDecomposedQueryId(),
                          qepToFail->getDecomposedQueryId());
                queryStatusListener->notifyQueryFailure(qepToFail->getSharedQueryId(),
                                                        qepToFail->getDecomposedQueryId(),
                                                        errorMessage);
                return qepToFail;
            }
            return nullptr;
        },
        std::move(qepToFail));
}

void AbstractQueryManager::notifySourceCompletion(DataSourcePtr source, QueryTerminationType terminationType) {
    std::unique_lock lock(queryMutex);
    //Check if a QEP exists for the datasource for which completion notification received.
    const auto& operatorId = source->getOperatorId();
    // Shutting down all sources by notifying source termination
    for (auto& entry : sourceToQEPMapping[operatorId]) {
        NES_TRACE("notifySourceCompletion operator id={} plan id={} subplan={}",
                  operatorId,
                  entry->getSharedQueryId(),
                  entry->getDecomposedQueryId());
        entry->notifySourceCompletion(source, terminationType);
        if (terminationType == QueryTerminationType::Graceful) {
            queryStatusListener->notifySourceTermination(entry->getSharedQueryId(),
                                                         entry->getDecomposedQueryId(),
                                                         operatorId,
                                                         QueryTerminationType::Graceful);
        }
    }
}

void AbstractQueryManager::notifyPipelineCompletion(DecomposedQueryId decomposedQueryId,
                                                    Execution::ExecutablePipelinePtr pipeline,
                                                    QueryTerminationType terminationType) {
    std::unique_lock lock(queryMutex);
    auto& qep = runningQEPs[decomposedQueryId];
    NES_ASSERT2_FMT(qep, "invalid query plan for pipeline " << pipeline->getPipelineId());
    qep->notifyPipelineCompletion(pipeline, terminationType);
}

void AbstractQueryManager::notifySinkCompletion(DecomposedQueryId decomposedQueryId,
                                                DataSinkPtr sink,
                                                QueryTerminationType terminationType) {
    std::unique_lock lock(queryMutex);
    auto& qep = runningQEPs[decomposedQueryId];
    NES_ASSERT2_FMT(qep, "invalid decomposed query plan id " << decomposedQueryId << " for sink " << sink->toString());
    qep->notifySinkCompletion(sink, terminationType);
}
}// namespace NES::Runtime
