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

#include <Compiler/JITCompilerBuilder.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/NetworkSink.hpp>
#include <Network/NetworkSource.hpp>
#include <Network/PartitionManager.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/LambdaSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <StatisticCollection/StatisticManager.hpp>
#include <StatisticCollection/StatisticStorage/DefaultStatisticStore.hpp>

#include <QueryCompiler/QueryCompilationRequest.hpp>// request = QueryCompilation::QueryCompilationRequest::create(..)
#include <QueryCompiler/QueryCompilationResult.hpp> // result = queryCompiler->compileQuery(request);
#include <QueryCompiler/QueryCompiler.hpp>          // member variable (QueryCompilation::QueryCompilerPtr queryCompiler)

#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <string>
#include <utility>

namespace NES::Runtime {

NodeEngine::NodeEngine(std::vector<PhysicalSourceTypePtr> physicalSources,
                       HardwareManagerPtr&& hardwareManager,
                       std::vector<BufferManagerPtr>&& bufferManagers,
                       QueryManagerPtr&& queryManager,
                       std::function<Network::NetworkManagerPtr(std::shared_ptr<NodeEngine>)>&& networkManagerCreator,
                       Network::PartitionManagerPtr&& partitionManager,
                       QueryCompilation::QueryCompilerPtr&& queryCompiler,
                       std::weak_ptr<AbstractQueryStatusListener>&& nesWorker,
                       OpenCLManagerPtr&& openCLManager,
                       WorkerId nodeEngineId,
                       uint64_t numberOfBuffersInGlobalBufferManager,
                       uint64_t numberOfBuffersInSourceLocalBufferPool,
                       uint64_t numberOfBuffersPerWorker,
                       bool sourceSharing)
    : nodeId(INVALID_WORKER_NODE_ID), physicalSources(std::move(physicalSources)), hardwareManager(std::move(hardwareManager)),
      bufferManagers(std::move(bufferManagers)), queryManager(std::move(queryManager)), queryCompiler(std::move(queryCompiler)),
      partitionManager(std::move(partitionManager)), nesWorker(std::move(nesWorker)),
      // TODO for now, we always use the DefaultStatisticStore. A configuration will be done with #4687
      statisticManager(Statistic::StatisticManager::create(Statistic::DefaultStatisticStore::create())),
      openCLManager(std::move(openCLManager)), nodeEngineId(nodeEngineId),
      numberOfBuffersInGlobalBufferManager(numberOfBuffersInGlobalBufferManager),
      numberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool),
      numberOfBuffersPerWorker(numberOfBuffersPerWorker), sourceSharing(sourceSharing) {

    NES_TRACE("Runtime() id={}", nodeEngineId);
    // here shared_from_this() does not work because of the machinery behind make_shared
    // as a result, we need to use a trick, i.e., a shared ptr that does not deallocate the node engine
    // plz make sure that ExchangeProtocol never leaks the impl pointer
    // TODO refactor to decouple the two components!
    networkManager = networkManagerCreator(std::shared_ptr<NodeEngine>(this, [](NodeEngine*) {
        // nop
    }));
    if (!this->queryManager->startThreadPool(numberOfBuffersPerWorker)) {
        NES_ERROR("error while start thread pool");
        throw Exceptions::RuntimeException("Error while starting thread pool");
    }
    NES_DEBUG("NodeEngine(): thread pool successfully started");

    isRunning.store(true);
}

NodeEngine::~NodeEngine() {
    NES_DEBUG("Destroying Runtime()");
    NES_ASSERT(stop(), "Cannot stop node engine");
}

bool NodeEngine::deployExecutableQueryPlan(const Execution::ExecutableQueryPlanPtr& executableQueryPlan) {
    std::unique_lock lock(engineMutex);
    NES_DEBUG("deployExecutableQueryPlan query using qep with sharedQueryId: {}", executableQueryPlan->getSharedQueryId());
    bool successRegister = registerExecutableQueryPlan(executableQueryPlan);
    if (!successRegister) {
        NES_ERROR("Runtime::deployExecutableQueryPlan: failed to register query");
        return false;
    }
    NES_DEBUG("Runtime::deployExecutableQueryPlan: successfully register query");

    bool successStart =
        startDecomposedQueryPlan(executableQueryPlan->getSharedQueryId(), executableQueryPlan->getDecomposedQueryId());
    if (!successStart) {
        NES_ERROR("Runtime::deployExecutableQueryPlan: failed to start query");
        return false;
    }
    NES_DEBUG("Runtime::deployExecutableQueryPlan: successfully start query");

    return true;
}

bool NodeEngine::registerDecomposableQueryPlan(const DecomposedQueryPlanPtr& decomposedQueryPlan) {
    SharedQueryId sharedQueryId = decomposedQueryPlan->getSharedQueryId();
    DecomposedQueryId decomposedQueryId = decomposedQueryPlan->getDecomposedQueryId();

    NES_INFO("Creating ExecutableQueryPlan for shared query plan {} and decomposed query plan {}",
             sharedQueryId,
             decomposedQueryId);

    auto request = QueryCompilation::QueryCompilationRequest::create(decomposedQueryPlan, inherited1::shared_from_this());
    request->enableDump();
    auto result = queryCompiler->compileQuery(request);
    try {
        auto executablePlan = result->getExecutableQueryPlan();
        return registerExecutableQueryPlan(executablePlan);
    } catch (std::exception const& error) {
        NES_ERROR("Error while building query execution plan: {}", error.what());
        return false;
    }
}

bool NodeEngine::registerExecutableQueryPlan(const Execution::ExecutableQueryPlanPtr& executableQueryPlan) {
    std::unique_lock lock(engineMutex);
    SharedQueryId sharedQueryId = executableQueryPlan->getSharedQueryId();
    DecomposedQueryId decomposedQueryId = executableQueryPlan->getDecomposedQueryId();
    NES_DEBUG("registerExecutableQueryPlan query with sharedQueryId= {} decomposedQueryId = {}",
              sharedQueryId,
              decomposedQueryId);
    NES_ASSERT(queryManager->isThreadPoolRunning(), "Registering query but thread pool not running");
    if (deployedExecutableQueryPlans.find(decomposedQueryId) == deployedExecutableQueryPlans.end()) {
        auto found = sharedQueryIdToDecomposedQueryPlanIds.find(sharedQueryId);
        if (found == sharedQueryIdToDecomposedQueryPlanIds.end()) {
            sharedQueryIdToDecomposedQueryPlanIds[sharedQueryId] = {decomposedQueryId};
            NES_DEBUG("register of QEP  {}  as a singleton", decomposedQueryId);
        } else {
            (*found).second.push_back(decomposedQueryId);
            NES_DEBUG("register of QEP  {}  added", decomposedQueryId);
        }
        /* We have to unlock here, as we do not want to hold the lock for the queryManager->registerQuery().
         * Otherwise, it can lead to the case, that we still hold the lock but another query wants to register itself on
         * this Node(1) and connect to Node(2). On Node(2), the queries are doing the reverse and thus, each query is
         * waiting on the other query to start all network sources and sinks. Leading to a deadlock!
         */
        lock.unlock();

        if (queryManager->registerExecutableQueryPlan(executableQueryPlan)) {
            // Here we have to lock again, as we are accessing deployedQEPs
            lock.lock();
            deployedExecutableQueryPlans[decomposedQueryId] = executableQueryPlan;
            NES_DEBUG("register of subqep  {}  succeeded", decomposedQueryId);
            return true;
        }
        NES_DEBUG("register of subqep  {}  failed", decomposedQueryId);
        return false;

    } else {
        NES_DEBUG("qep already exists. register failed {}", decomposedQueryId);
        return false;
    }
}

bool NodeEngine::startDecomposedQueryPlan(SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId) {
    std::unique_lock lock(engineMutex);
    NES_DEBUG("startDecomposedQuery= {}", sharedQueryId);
    if (sharedQueryIdToDecomposedQueryPlanIds.find(sharedQueryId) != sharedQueryIdToDecomposedQueryPlanIds.end()) {
        std::vector<DecomposedQueryId> decomposedQueryPlanIds = sharedQueryIdToDecomposedQueryPlanIds[sharedQueryId];
        if (decomposedQueryPlanIds.empty()) {
            NES_ERROR("Unable to find qep ids for the query {}. Start failed.", sharedQueryId);
            return false;
        }

        if (std::find(decomposedQueryPlanIds.begin(), decomposedQueryPlanIds.end(), decomposedQueryId)
            == decomposedQueryPlanIds.end()) {
            NES_ERROR("Unable to find qep with id {} for the shared query {}. Start failed.", decomposedQueryId, sharedQueryId);
            return false;
        }

        try {
            if (queryManager->startExecutableQueryPlan(deployedExecutableQueryPlans[decomposedQueryId])) {
                NES_DEBUG("start of QEP  {}  succeeded", decomposedQueryId);
            } else {
                NES_DEBUG("start of QEP  {}  failed", decomposedQueryId);
                return false;
            }
        } catch (std::exception const& exception) {
            NES_ERROR("Got exception while starting query {}", exception.what());
        }

        return true;
    }
    NES_ERROR("qep does not exists. start failed for query={}", sharedQueryId);
    return false;
}

bool NodeEngine::undeployDecomposedQueryPlan(SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId) {
    std::unique_lock lock(engineMutex);
    NES_DEBUG("UndeployQuery query= {}", sharedQueryId);
    bool successStop = stopDecomposedQueryPlan(sharedQueryId, decomposedQueryId);
    if (!successStop) {
        NES_ERROR("Runtime::undeployDecomposedQueryPlan: failed to stop query");
        return false;
    }
    NES_DEBUG("Runtime::undeployDecomposedQueryPlan: successfully stop query");

    bool successUnregister = unregisterDecomposedQueryPlan(sharedQueryId, decomposedQueryId);
    if (!successUnregister) {
        NES_ERROR("Runtime::undeployDecomposedQueryPlan: failed to unregister query");
        return false;
    }
    NES_DEBUG("Runtime::undeployDecomposedQueryPlan: successfully unregister query");
    return true;
}

bool NodeEngine::unregisterDecomposedQueryPlan(SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId) {
    std::unique_lock lock(engineMutex);
    NES_DEBUG("unregisterDecomposedQuery query= {}", sharedQueryId);
    bool ret = true;
    if (sharedQueryIdToDecomposedQueryPlanIds.contains(sharedQueryId)) {
        auto& registeredDecomposedQueryPlanIds = sharedQueryIdToDecomposedQueryPlanIds[sharedQueryId];
        if (registeredDecomposedQueryPlanIds.empty()) {
            NES_ERROR("Unable to find any registered decomposed query plans for the shared query {}. Unregister failed.",
                      sharedQueryId);
            return false;
        }

        auto found =
            std::find(registeredDecomposedQueryPlanIds.begin(), registeredDecomposedQueryPlanIds.end(), decomposedQueryId);
        if (found == registeredDecomposedQueryPlanIds.end()) {
            NES_ERROR(
                "Unable to locate any decomposed query plan with id {} registed for the shared query {}. Unregister failed.",
                decomposedQueryId,
                sharedQueryId);
            return false;
        }

        auto qep = deployedExecutableQueryPlans[decomposedQueryId];
        bool isStopped = false;
        switch (qep->getStatus()) {
            case Execution::ExecutableQueryPlanStatus::Created:
            case Execution::ExecutableQueryPlanStatus::Deployed:
            case Execution::ExecutableQueryPlanStatus::Running: {
                NES_DEBUG("unregister of query  {}  is not Stopped... stopping now", decomposedQueryId);
                isStopped = queryManager->stopExecutableQueryPlan(qep, Runtime::QueryTerminationType::HardStop);
                break;
            }
            default: {
                isStopped = true;
                break;
            };
        }
        NES_DEBUG("unregister of query  {} : current status is stopped= {}", decomposedQueryId, isStopped);
        if (isStopped && queryManager->unregisterExecutableQueryPlan(qep)) {
            deployedExecutableQueryPlans.erase(decomposedQueryId);
            NES_DEBUG("unregister of query  {}  succeeded", decomposedQueryId);
        } else {
            NES_ERROR("unregister of QEP {} failed", decomposedQueryId);
            return false;
        }

        // Update the registered decomposed query plan
        registeredDecomposedQueryPlanIds.erase(found);
        sharedQueryIdToDecomposedQueryPlanIds[sharedQueryId] = registeredDecomposedQueryPlanIds;
        return true;
    }

    NES_ERROR("qep does not exists. unregister failed for shared query id {}", sharedQueryId);
    return false;
}

bool NodeEngine::stopDecomposedQueryPlan(SharedQueryId sharedQueryId,
                                         DecomposedQueryId decomposedQueryId,
                                         Runtime::QueryTerminationType terminationType) {
    std::unique_lock lock(engineMutex);
    NES_WARNING("Runtime:stopDecomposedQueryPlan for qep with shared query id = {} and decomposed query id = {}  termination= {}",
                sharedQueryId,
                decomposedQueryId,
                terminationType);
    auto it = sharedQueryIdToDecomposedQueryPlanIds.find(sharedQueryId);
    if (it != sharedQueryIdToDecomposedQueryPlanIds.end()) {
        std::vector<DecomposedQueryId> decomposedQueryPlanIds = it->second;
        if (decomposedQueryPlanIds.empty()) {
            NES_ERROR("Unable to find qep ids for the query {}. Stop failed.", sharedQueryId);
            return false;
        }

        if (std::find(decomposedQueryPlanIds.begin(), decomposedQueryPlanIds.end(), decomposedQueryId)
            == decomposedQueryPlanIds.end()) {
            NES_ERROR("Unable to find qep with id {} for the shared query {}. Start failed.", decomposedQueryId, sharedQueryId);
            return false;
        }

        switch (terminationType) {
            case QueryTerminationType::Graceful:
            case QueryTerminationType::HardStop: {
                try {
                    if (queryManager->stopExecutableQueryPlan(deployedExecutableQueryPlans[decomposedQueryId], terminationType)) {
                        queryManager->resetQueryStatistics(decomposedQueryId);
                        NES_DEBUG("stop of QEP  {}  succeeded", decomposedQueryId);
                        return true;
                    } else {
                        NES_ERROR("stop of QEP {} failed", decomposedQueryId);
                        return false;
                    }
                } catch (std::exception const& exception) {
                    NES_ERROR("Got exception while stopping query {}", exception.what());
                    return false;// handle this better!
                }
            }
            case QueryTerminationType::Failure: {
                try {
                    if (queryManager->failExecutableQueryPlan(deployedExecutableQueryPlans[decomposedQueryId])) {
                        NES_DEBUG("failure of QEP  {}  succeeded", decomposedQueryId);
                        return true;
                    } else {
                        NES_ERROR("failure of QEP {} failed", decomposedQueryId);
                        return false;
                    }
                } catch (std::exception const& exception) {
                    NES_ERROR("Got exception while stopping query {}", exception.what());
                    return false;// handle this better!
                }
            }
            case QueryTerminationType::Invalid: NES_NOT_IMPLEMENTED();
        }
        return true;
    }
    NES_ERROR("qep does not exists. stop failed {}", sharedQueryId);
    return false;
}

QueryManagerPtr NodeEngine::getQueryManager() { return queryManager; }

bool NodeEngine::stop(bool markQueriesAsFailed) {
    //TODO: add check if still queryIdAndCatalogEntryMapping are running
    //TODO @Steffen: does it make sense to have force stop still?
    //TODO @all: imho, when this method terminates, nothing must be running still and all resources must be returned to the engine
    //TODO @all: error handling, e.g., is it an error if the query is stopped but not non-deployed? @Steffen?

    bool expected = true;
    if (!isRunning.compare_exchange_strong(expected, false)) {
        NES_WARNING("Runtime::stop: engine already stopped");
        return true;
    }
    NES_DEBUG("Runtime::stop: going to stop the node engine");
    std::unique_lock lock(engineMutex);
    bool withError = false;

    // release all deployed queryIdAndCatalogEntryMapping
    for (auto it = deployedExecutableQueryPlans.begin(); it != deployedExecutableQueryPlans.end();) {
        auto& [querySubPlanId, queryExecutionPlan] = *it;
        try {
            if (markQueriesAsFailed) {
                if (queryManager->failExecutableQueryPlan(queryExecutionPlan)) {
                    NES_DEBUG("fail of QEP  {}  succeeded", querySubPlanId);
                } else {
                    NES_ERROR("fail of QEP {} failed", querySubPlanId);
                    withError = true;
                }
            } else {
                if (queryManager->stopExecutableQueryPlan(queryExecutionPlan)) {
                    NES_DEBUG("stop of QEP  {}  succeeded", querySubPlanId);
                } else {
                    NES_ERROR("stop of QEP {} failed", querySubPlanId);
                    withError = true;
                }
            }
        } catch (std::exception const& err) {
            NES_ERROR("stop of QEP {} failed: {}", querySubPlanId, err.what());
            withError = true;
        }
        try {
            if (queryManager->unregisterExecutableQueryPlan(queryExecutionPlan)) {
                NES_DEBUG("unregisterExecutableQueryPlan of QEP  {}  succeeded", querySubPlanId);
                it = deployedExecutableQueryPlans.erase(it);
            } else {
                NES_ERROR("unregisterExecutableQueryPlan of QEP {} failed", querySubPlanId);
                withError = true;
                ++it;
            }
        } catch (std::exception const& err) {
            NES_ERROR("unregisterExecutableQueryPlan of QEP {} failed: {}", querySubPlanId, err.what());
            withError = true;
            ++it;
        }
    }
    // release components
    // TODO do not touch the sequence here as it will lead to errors in the shutdown sequence
    deployedExecutableQueryPlans.clear();
    sharedQueryIdToDecomposedQueryPlanIds.clear();
    queryManager->destroy();
    networkManager->destroy();
    partitionManager->clear();
    for (auto&& bufferManager : bufferManagers) {
        bufferManager->destroy();
    }
    nesWorker.reset();// break cycle
    return !withError;
}

BufferManagerPtr NodeEngine::getBufferManager(uint32_t bufferManagerIndex) const {
    NES_ASSERT2_FMT(bufferManagerIndex < bufferManagers.size(), "invalid buffer manager index=" << bufferManagerIndex);
    return bufferManagers[bufferManagerIndex];
}

WorkerId NodeEngine::getWorkerId() { return nodeEngineId; }

Network::NetworkManagerPtr NodeEngine::getNetworkManager() { return networkManager; }

AbstractQueryStatusListenerPtr NodeEngine::getQueryStatusListener() { return nesWorker; }

HardwareManagerPtr NodeEngine::getHardwareManager() const { return hardwareManager; }

Execution::ExecutableQueryPlanStatus NodeEngine::getQueryStatus(SharedQueryId sharedQueryId) {
    std::unique_lock lock(engineMutex);
    if (sharedQueryIdToDecomposedQueryPlanIds.find(sharedQueryId) != sharedQueryIdToDecomposedQueryPlanIds.end()) {
        std::vector<DecomposedQueryId> decomposedQueryPlanIds = sharedQueryIdToDecomposedQueryPlanIds[sharedQueryId];
        if (decomposedQueryPlanIds.empty()) {
            NES_ERROR("Unable to find qep ids for the query {}. Start failed.", sharedQueryId);
            return Execution::ExecutableQueryPlanStatus::Invalid;
        }

        for (auto decomposedQueryId : decomposedQueryPlanIds) {
            //FIXME: handle vector of statistics properly in #977
            return deployedExecutableQueryPlans[decomposedQueryId]->getStatus();
        }
    }
    return Execution::ExecutableQueryPlanStatus::Invalid;
}

void NodeEngine::onDataBuffer(Network::NesPartition, TupleBuffer&) {
    // nop :: kept as legacy
}

void NodeEngine::onEvent(NES::Network::NesPartition, NES::Runtime::BaseEvent&) {
    // nop :: kept as legacy
}

void NodeEngine::onEndOfStream(Network::Messages::EndOfStreamMessage) {
    // nop :: kept as legacy
}

void NodeEngine::onServerError(Network::Messages::ErrorMessage err) {

    switch (err.getErrorType()) {
        case Network::Messages::ErrorType::PartitionNotRegisteredError: {
            NES_WARNING("Unable to find the NES Partition {}", err.getChannelId());
            break;
        }
        case Network::Messages::ErrorType::DeletedPartitionError: {
            NES_WARNING("Requesting deleted NES Partition {}", err.getChannelId());
            break;
        }
        case Network::Messages::ErrorType::VersionMismatchError: {
            NES_INFO("Node {} encountered server error: Version mismatch for requested partition {}", nodeId, err.getChannelId());
            break;
        }
        default: {
            NES_ASSERT(false, err.getErrorTypeAsString());
            break;
        }
    }
}

void NodeEngine::onChannelError(Network::Messages::ErrorMessage err) {
    switch (err.getErrorType()) {
        case Network::Messages::ErrorType::PartitionNotRegisteredError: {
            NES_WARNING("Unable to find the NES Partition {}", err.getChannelId());
            break;
        }
        case Network::Messages::ErrorType::DeletedPartitionError: {
            NES_WARNING("Requesting deleted NES Partition {}", err.getChannelId());
            break;
        }
        case Network::Messages::ErrorType::VersionMismatchError: {
            NES_INFO("Expected version is not running yet for channel {}", err.getChannelId());
            break;
        }
        default: {
            NES_THROW_RUNTIME_ERROR(err.getErrorTypeAsString());
            break;
        }
    }
}

std::vector<QueryStatisticsPtr> NodeEngine::getQueryStatistics(SharedQueryId sharedQueryId) {
    NES_INFO("QueryManager: Get query statistics for query {}", sharedQueryId);
    std::unique_lock lock(engineMutex);
    std::vector<QueryStatisticsPtr> queryStatistics;

    NES_TRACE("QueryManager: Check if query is registered");
    auto foundQuerySubPlanIds = sharedQueryIdToDecomposedQueryPlanIds.find(sharedQueryId);
    NES_TRACE("Found members = {}", foundQuerySubPlanIds->second.size());
    if (foundQuerySubPlanIds == sharedQueryIdToDecomposedQueryPlanIds.end()) {
        NES_ERROR("AbstractQueryManager::getQueryStatistics: query does not exists {}", sharedQueryId);
        return queryStatistics;
    }

    NES_TRACE("QueryManager: Extracting query execution ids for the input query {}", sharedQueryId);
    std::vector<DecomposedQueryId> decomposedQueryPlanIds = (*foundQuerySubPlanIds).second;
    for (auto decomposedQueryId : decomposedQueryPlanIds) {
        queryStatistics.emplace_back(queryManager->getQueryStatistics(decomposedQueryId));
    }
    return queryStatistics;
}

std::vector<QueryStatistics> NodeEngine::getQueryStatistics(bool withReset) {
    std::unique_lock lock(engineMutex);
    std::vector<QueryStatistics> queryStatistics;

    for (auto& plan : sharedQueryIdToDecomposedQueryPlanIds) {
        NES_TRACE("QueryManager: Extracting query execution ids for the input query {}", plan.first);
        std::vector<DecomposedQueryId> querySubPlanIds = plan.second;
        for (auto querySubPlanId : querySubPlanIds) {
            NES_TRACE("querySubPlanId={} stat= {}",
                      querySubPlanId,
                      queryManager->getQueryStatistics(querySubPlanId)->getQueryStatisticsAsString());

            queryStatistics.push_back(queryManager->getQueryStatistics(querySubPlanId).operator*());
            if (withReset) {
                queryManager->getQueryStatistics(querySubPlanId)->clear();
            }
        }
    }

    return queryStatistics;
}

Network::PartitionManagerPtr NodeEngine::getPartitionManager() { return partitionManager; }

std::vector<DecomposedQueryId> NodeEngine::getDecomposedQueryIds(SharedQueryId sharedQueryId) {
    auto iterator = sharedQueryIdToDecomposedQueryPlanIds.find(sharedQueryId);
    if (iterator != sharedQueryIdToDecomposedQueryPlanIds.end()) {
        return iterator->second;
    } else {
        return {};
    }
}

void NodeEngine::onFatalError(int signalNumber, std::string callstack) {
    if (callstack.empty()) {
        NES_ERROR("onFatalError: signal [{}] error [{}] (enable NES_DEBUG to view stacktrace)", signalNumber, strerror(errno));
        std::cerr << "Runtime failed fatally" << std::endl;// it's necessary for testing and it wont harm us to write to stderr
        std::cerr << "Error: " << strerror(errno) << std::endl;
        std::cerr << "Signal: " << std::to_string(signalNumber) << std::endl;
    } else {
        NES_ERROR("onFatalError: signal [{}] error [{}] callstack {}", signalNumber, strerror(errno), callstack);
        std::cerr << "Runtime failed fatally" << std::endl;// it's necessary for testing and it won't harm us to write to stderr
        std::cerr << "Error: " << strerror(errno) << std::endl;
        std::cerr << "Signal: " << std::to_string(signalNumber) << std::endl;
        std::cerr << "Callstack:\n " << callstack << std::endl;
    }
#ifdef ENABLE_CORE_DUMPER
    detail::createCoreDump();
#endif
}

void NodeEngine::onFatalException(const std::shared_ptr<std::exception> exception, std::string callstack) {
    if (callstack.empty()) {
        NES_ERROR("onFatalException: exception=[{}] (enable NES_DEBUG to view stacktrace)", exception->what());
        std::cerr << "Runtime failed fatally" << std::endl;
        std::cerr << "Error: " << strerror(errno) << std::endl;
        std::cerr << "Exception: " << exception->what() << std::endl;
    } else {
        NES_ERROR("onFatalException: exception=[{}] callstack={}", exception->what(), callstack);
        std::cerr << "Runtime failed fatally" << std::endl;
        std::cerr << "Error: " << strerror(errno) << std::endl;
        std::cerr << "Exception: " << exception->what() << std::endl;
        std::cerr << "Callstack:\n " << callstack << std::endl;
    }
#ifdef ENABLE_CORE_DUMPER
    detail::createCoreDump();
#endif
}

const std::vector<PhysicalSourceTypePtr>& NodeEngine::getPhysicalSourceTypes() const { return physicalSources; }

std::shared_ptr<const Execution::ExecutableQueryPlan>
NodeEngine::getExecutableQueryPlan(DecomposedQueryId decomposedQueryId) const {
    std::unique_lock lock(engineMutex);
    auto iterator = deployedExecutableQueryPlans.find(decomposedQueryId);
    if (iterator != deployedExecutableQueryPlans.end()) {
        return iterator->second;
    }
    return nullptr;
}

bool NodeEngine::bufferData(DecomposedQueryId decomposedQueryId, OperatorId uniqueNetworkSinkDescriptorId) {
    //TODO: #2412 add error handling/return false in some cases
    NES_DEBUG("NodeEngine: Received request to buffer Data on network Sink");
    std::unique_lock lock(engineMutex);
    if (deployedExecutableQueryPlans.find(decomposedQueryId) == deployedExecutableQueryPlans.end()) {
        NES_DEBUG("Deployed QEP with ID:  {}  not found", decomposedQueryId);
        return false;
    } else {
        auto qep = deployedExecutableQueryPlans.at(decomposedQueryId);
        auto sinks = qep->getSinks();
        //make sure that query sub plan has network sink with specified id
        auto it = std::find_if(sinks.begin(), sinks.end(), [uniqueNetworkSinkDescriptorId](const DataSinkPtr& dataSink) {
            Network::NetworkSinkPtr networkSink = std::dynamic_pointer_cast<Network::NetworkSink>(dataSink);
            return networkSink && networkSink->getUniqueNetworkSinkDescriptorId() == uniqueNetworkSinkDescriptorId;
        });
        if (it != sinks.end()) {
            auto networkSink = *it;
            //below code will be added in #2395
            //ReconfigurationMessage message = ReconfigurationMessage(querySubPlanId,BufferData,networkSink);
            //queryManager->addReconfigurationMessage(querySubPlanId,message,true);
            NES_NOT_IMPLEMENTED();
            return true;
        }
        //query sub plan did not have network sink with specified id
        NES_DEBUG("Query Sub Plan with ID {} did not contain a Network Sink with a Descriptor with ID {}",
                  decomposedQueryId,
                  uniqueNetworkSinkDescriptorId);
        return false;
    }
}

bool NodeEngine::updateNetworkSink(WorkerId newNodeId,
                                   const std::string& newHostname,
                                   uint32_t newPort,
                                   DecomposedQueryId decomposedQueryId,
                                   OperatorId uniqueNetworkSinkDescriptorId) {
    //TODO: #2412 add error handling/return false in some cases
    NES_ERROR("NodeEngine: Received request to update Network Sink");
    Network::NodeLocation newNodeLocation(newNodeId, newHostname, newPort);
    std::unique_lock lock(engineMutex);
    if (deployedExecutableQueryPlans.find(decomposedQueryId) == deployedExecutableQueryPlans.end()) {
        NES_DEBUG("Deployed QEP with ID:  {}  not found", decomposedQueryId);
        return false;
    } else {
        auto qep = deployedExecutableQueryPlans.at(decomposedQueryId);
        auto networkSinks = qep->getSinks();
        //make sure that query sub plan has network sink with specified id
        auto it =
            std::find_if(networkSinks.begin(), networkSinks.end(), [uniqueNetworkSinkDescriptorId](const DataSinkPtr& dataSink) {
                Network::NetworkSinkPtr networkSink = std::dynamic_pointer_cast<Network::NetworkSink>(dataSink);
                return networkSink && networkSink->getUniqueNetworkSinkDescriptorId() == uniqueNetworkSinkDescriptorId;
            });
        if (it != networkSinks.end()) {
            auto networkSink = *it;
            //below code will be added in #2402
            //ReconfigurationMessage message = ReconfigurationMessage(querySubPlanId,UpdateSinks,networkSink, newNodeLocation);
            //queryManager->addReconfigurationMessage(querySubPlanId,message,true);
            NES_NOT_IMPLEMENTED();
            return true;
        }
        //query sub plan did not have network sink with specified id
        NES_DEBUG("Query Sub Plan with ID {} did not contain a Network Sink with a Descriptor with ID {}",
                  decomposedQueryId,
                  uniqueNetworkSinkDescriptorId);
        return false;
    }
}

bool NodeEngine::reconfigureSubPlan(DecomposedQueryPlanPtr& reconfiguredDecomposedQueryPlan) {
    std::unique_lock lock(engineMutex);
    NES_DEBUG("Received for shared query plan {} the decomposed query plan {} for reconfiguration.",
              reconfiguredDecomposedQueryPlan->getSharedQueryId(),
              reconfiguredDecomposedQueryPlan->getDecomposedQueryId());
    auto deployedPlanIterator = deployedExecutableQueryPlans.find(reconfiguredDecomposedQueryPlan->getDecomposedQueryId());

    //if not running sub query plan with the given id exists, return false
    if (deployedPlanIterator == deployedExecutableQueryPlans.end()) {
        return false;
    }
    auto deployedPlan = deployedPlanIterator->second;

    /* iterator over all network sinks of the running plan and apply the new descriptors. If the version number
     * of the new descriptor is the same as the running version, nothing will be changed */
    for (auto& sink : deployedPlan->getSinks()) {
        auto networkSink = std::dynamic_pointer_cast<Network::NetworkSink>(sink);
        if (networkSink != nullptr) {
            for (auto& reconfiguredSink : reconfiguredDecomposedQueryPlan->getSinkOperators()) {
                auto reconfiguredNetworkSinkDescriptor =
                    std::dynamic_pointer_cast<const Network::NetworkSinkDescriptor>(reconfiguredSink->getSinkDescriptor());
                if (reconfiguredNetworkSinkDescriptor
                    && reconfiguredNetworkSinkDescriptor->getUniqueId() == networkSink->getUniqueNetworkSinkDescriptorId()) {
                    NES_DEBUG("Reconfiguring the network sink {} with new descriptor for shared query plan {} and the decomposed "
                              "query plan {}.",
                              reconfiguredNetworkSinkDescriptor->getUniqueId(),
                              reconfiguredDecomposedQueryPlan->getSharedQueryId(),
                              reconfiguredDecomposedQueryPlan->getDecomposedQueryId());
                    networkSink->scheduleNewDescriptor(*reconfiguredNetworkSinkDescriptor);
                }
            }
        }
    }
    // iterate over all network sources and apply the reconfigurations
    for (auto& source : deployedPlan->getSources()) {
        auto networkSource = std::dynamic_pointer_cast<Network::NetworkSource>(source);
        if (networkSource != nullptr) {
            for (auto& reconfiguredSource : reconfiguredDecomposedQueryPlan->getSourceOperators()) {
                auto reconfiguredNetworkSourceDescriptor =
                    std::dynamic_pointer_cast<const Network::NetworkSourceDescriptor>(reconfiguredSource->getSourceDescriptor());
                if (reconfiguredNetworkSourceDescriptor->getUniqueId() == networkSource->getUniqueId()) {
                    NES_DEBUG("Reconfiguring the network source {} with new descriptor for shared query plan {} and the "
                              "decomposed query plan {}.",
                              reconfiguredNetworkSourceDescriptor->getUniqueId(),
                              reconfiguredDecomposedQueryPlan->getSharedQueryId(),
                              reconfiguredDecomposedQueryPlan->getDecomposedQueryId());
                    networkSource->scheduleNewDescriptor(*reconfiguredNetworkSourceDescriptor);
                }
            }
        }
    }
    return true;
}

Monitoring::MetricStorePtr NodeEngine::getMetricStore() { return metricStore; }
void NodeEngine::setMetricStore(Monitoring::MetricStorePtr metricStore) {
    NES_ASSERT(metricStore != nullptr, "NodeEngine: MetricStore is null.");
    this->metricStore = metricStore;
}
WorkerId NodeEngine::getNodeId() const { return nodeId; }
void NodeEngine::setNodeId(const WorkerId NodeId) { nodeId = NodeId; }

void NodeEngine::updatePhysicalSources(const std::vector<PhysicalSourceTypePtr>& physicalSources) {
    this->physicalSources = std::move(physicalSources);
}

const OpenCLManagerPtr NodeEngine::getOpenCLManager() const { return openCLManager; }

const Statistic::StatisticManagerPtr NodeEngine::getStatisticManager() const { return statisticManager; }

}// namespace NES::Runtime
