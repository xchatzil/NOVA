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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_NODEENGINE_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_NODEENGINE_HPP_

#include <Exceptions/ErrorListener.hpp>
#include <Network/ExchangeProtocolListener.hpp>
#include <Network/NetworkForwardRefs.hpp>
#include <Runtime/Execution/ExecutableQueryPlanStatus.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <StatisticCollection/StatisticManager.hpp>
#include <Util/VirtualEnableSharedFromThis.hpp>
#include <iostream>
#include <map>
#include <mutex>
#include <pthread.h>
#include <string>
#include <unistd.h>
#include <unordered_set>
#include <vector>

namespace NES {

class NesWorker;
using NesWorkerPtr = std::shared_ptr<NesWorker>;

class DecomposedQueryPlan;
using DecomposedQueryPlanPtr = std::shared_ptr<DecomposedQueryPlan>;

class PhysicalSourceType;
using PhysicalSourceTypePtr = std::shared_ptr<PhysicalSourceType>;

namespace Monitoring {
class AbstractMetricStore;
using MetricStorePtr = std::shared_ptr<AbstractMetricStore>;
}//namespace Monitoring

namespace Runtime {

/**
 * @brief this class represents the interface and entrance point into the
 * query processing part of NES. It provides basic functionality
 * such as deploying, undeploying, starting, and stopping.
 *
 */
class NodeEngine : public Network::ExchangeProtocolListener,
                   public NES::detail::virtual_enable_shared_from_this<NodeEngine>,
                   public Exceptions::ErrorListener {
    // virtual_enable_shared_from_this necessary for double inheritance of enable_shared_from_this
    using inherited0 = Network::ExchangeProtocolListener;
    using inherited1 = virtual_enable_shared_from_this<NodeEngine>;
    using inherited2 = ErrorListener;

    friend class NodeEngineBuilder;

  public:
    enum class NodeEngineQueryStatus : uint8_t { started, stopped, registered };

    virtual ~NodeEngine() override;

    NodeEngine() = delete;
    NodeEngine(const NodeEngine&) = delete;
    NodeEngine& operator=(const NodeEngine&) = delete;

    /**
     * @brief signal handler: behaviour not clear yet!
     * @param signalNumber
     * @param callstack
     */
    void onFatalError(int signalNumber, std::string callstack) override;

    /**
     * @brief exception handler: behaviour not clear yet!
     * @param exception
     * @param callstack
     */
    void onFatalException(std::shared_ptr<std::exception> exception, std::string callstack) override;

    /**
     * @brief deploy registers and starts a query
     * @param executableQueryPlan the executable query plan to deploy
     * @return true if succeeded, else false
     */
    [[nodiscard]] bool deployExecutableQueryPlan(const Execution::ExecutableQueryPlanPtr& executableQueryPlan);

    /**
     * @brief registers an executable query plan
     * @param executableQueryPlan: executable query plan to register
     * @return true if succeeded, else false
     */
    [[nodiscard]] bool registerExecutableQueryPlan(const Execution::ExecutableQueryPlanPtr& executableQueryPlan);

    /**
     * @brief Stops and undeploy a decomposed query plan
     * @param sharedQueryId the shared query plan id that is served by the decomposed query plan
     * @param decomposedQueryId id of the decomposed query plan to undeploy
     * @return true if succeeded, else false
     */
    [[nodiscard]] bool undeployDecomposedQueryPlan(SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId);

    /**
     * @brief registers a decomposed query plan
     * @param decomposedQueryPlan: the decomposed query plan to be registered
     * @return true if succeeded, else false
     */
    [[nodiscard]] bool registerDecomposableQueryPlan(const DecomposedQueryPlanPtr& decomposedQueryPlan);

    /**
     * @brief unregisters a decomposed query
     * @param sharedQueryId: id of the shared query which is served by the decomposed query plan
     * @param decomposedQueryId: id of the decomposed query plan to be unregistered
     * @return true if succeeded, else false
     */
    [[nodiscard]] bool unregisterDecomposedQueryPlan(SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId);

    /**
     * @brief method to start a already deployed query
     * @note if query is not deploy, false is returned
     * @param sharedQueryId: id of the shared query which is served by the decomposed query plan
     * @param decomposedQueryId: id of the decomposed query plan to be started
     * @return bool indicating success
     */
    [[nodiscard]] bool startDecomposedQueryPlan(SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId);

    /**
     * @brief method to stop a decomposed query plan
     * @param sharedQueryId: id of the shared query which is served by the decomposed query plan
     * @param decomposedQueryId: id of the decomposed query plan to be stopped
     * @param graceful hard or soft termination
     * @return bool indicating success
     */
    [[nodiscard]] bool
    stopDecomposedQueryPlan(SharedQueryId sharedQueryId,
                            DecomposedQueryId decomposedQueryId,
                            Runtime::QueryTerminationType terminationType = Runtime::QueryTerminationType::HardStop);

    /**
     * @brief method to trigger the buffering of data on a NetworkSink of a Query Sub Plan with the given id
     * @param decomposedQueryId : the id of the Query Sub Plan to which the Network Sink belongs to
     * @param uniqueNetworkSinkDescriptorId : the id of the Network Sink Descriptor. Helps identify the Network Sink on which to buffer data
     * @return bool indicating success
     */
    bool bufferData(DecomposedQueryId decomposedQueryId, OperatorId uniqueNetworkSinkDescriptorId);

    /**
     * @brief method to trigger the reconfiguration of a NetworkSink so that it points to a new downstream node.
     * @param newNodeId : the id of the new node
     * @param newHostname : the hostname of the new node
     * @param newPort : the port of the new node
     * @param decomposedQueryId : the id of the Query Sub Plan to which the Network Sink belongs to
     * @param uniqueNetworkSinkDescriptorId : the id of the Network Sink Descriptor. Helps identify the Network Sink to reconfigure.
     * @return bool indicating success
     */
    bool updateNetworkSink(WorkerId newNodeId,
                           const std::string& newHostname,
                           uint32_t newPort,
                           DecomposedQueryId decomposedQueryId,
                           OperatorId uniqueNetworkSinkDescriptorId);

    /**
     * @brief release all resource of the node engine
     * @param withError true if the node engine stopped with an error
     */
    [[nodiscard]] bool stop(bool markQueriesAsFailed = false);

    /**
     * @brief getter of query manager
     * @return query manager
     */
    QueryManagerPtr getQueryManager();

    /**
     * @brief getter of buffer manager for the i-th numa region (defaul: 0)
     * @return bufferManager
     */
    BufferManagerPtr getBufferManager(uint32_t bufferManagerIndex = 0) const;

    /**
    * @brief getter of node id
    * @return WorkerId
    */
    WorkerId getWorkerId();

    /**
     * @brief getter of network manager
     * @return network manager
     */
    Network::NetworkManagerPtr getNetworkManager();

    /**
     * @brief getter of query status listener
     * @return return the query status listener
     */
    AbstractQueryStatusListenerPtr getQueryStatusListener();

    /**
     * @return return the status of a query
     */
    Execution::ExecutableQueryPlanStatus getQueryStatus(SharedQueryId sharedQueryId);

    /**
    * @brief method to return the query statistics
    * @param sharedQueryId of the query
    * @return vector of queryStatistics
    */
    std::vector<QueryStatisticsPtr> getQueryStatistics(SharedQueryId sharedQueryId);

    /**
     * @brief method to return the query statistics
     * @param withReset specifies if the statistics is deleted after reading (so we start with 0)
     * @return vector of queryStatistics
    */
    std::vector<QueryStatistics> getQueryStatistics(bool withReset = false);

    Network::PartitionManagerPtr getPartitionManager();

    ///// Network Callback //////

    /**
     * @brief this callback is called once a tuple buffer arrives on the network manager
     * for a given nes partition
     */
    void onDataBuffer(Network::NesPartition, TupleBuffer&) override;

    /**
     * @brief this callback is called once a tuple buffer arrives on the network manager
     * for a given nes partition
     */
    void onEvent(Network::NesPartition, Runtime::BaseEvent&) override;

    /**
     * @brief this callback is called once an end of stream message arrives
     */
    void onEndOfStream(Network::Messages::EndOfStreamMessage) override;

    /**
     * @brief this callback is called once an error is raised on the server side
     */
    void onServerError(Network::Messages::ErrorMessage) override;

    /**
     * @brief this callback is called once an error is raised on the channel(client) side
     */
    void onChannelError(Network::Messages::ErrorMessage) override;

    /**
     * @brief Provide the hardware manager
     * @return the hardware manager
     */
    HardwareManagerPtr getHardwareManager() const;

    /**
     * @brief Get physical sources configured
     * @return list of physical sources
     */
    const std::vector<PhysicalSourceTypePtr>& getPhysicalSourceTypes() const;

    /**
     * @brief finds executable query plan for a given sub query id
     * @param decomposedQueryId query sub plan id
     * @return executable query plan
     */
    std::shared_ptr<const Execution::ExecutableQueryPlan> getExecutableQueryPlan(DecomposedQueryId decomposedQueryId) const;

    /**
     * @brief finds sub query ids for a given query id
     * @param sharedQueryId query id
     * @return vector of subQueryIds
     */
    std::vector<DecomposedQueryId> getDecomposedQueryIds(SharedQueryId sharedQueryId);

    /**
     * Getter for the metric store
     * @return the metric store
     */
    Monitoring::MetricStorePtr getMetricStore();

    /**
     * Setter for the metric store
     * @param metricStore
     */
    void setMetricStore(Monitoring::MetricStorePtr metricStore);

    /**
     * Getter for node Id
     * @return the node id
     */
    WorkerId getNodeId() const;

    /**
     * Setter for node ID
     * @param NodeId
     */
    void setNodeId(const WorkerId NodeId);

    /**
     * @brief Updates the physical sources on the node engine
     * @param physicalSources
     */
    void updatePhysicalSources(const std::vector<PhysicalSourceTypePtr>& physicalSources);

    const OpenCLManagerPtr getOpenCLManager() const;

    const Statistic::StatisticManagerPtr getStatisticManager() const;

    /**
     * @return applies reconfigurations to the sources or sinks of a sub plan. Reconfigured sources will start expecting
     * connections from a new upstream sink. Reconfigured sinks will scheduled a pending change of the downstream source
     * to which they send their data.
     * @param reconfiguredDecomposedQueryPlan A query plan containing source or sink descriptors which contain the updated
     * sender/receiver date.
     * @return true if a running sub query with a matching id was found and reconfigured. False if the id of the supplied
     * plan did not match any running sub query
     */
    bool reconfigureSubPlan(DecomposedQueryPlanPtr& reconfiguredDecomposedQueryPlan);

  public:
    /**
     * @brief Create a node engine and gather node information
     * and initialize QueryManager, BufferManager and ThreadPool
     */
    explicit NodeEngine(std::vector<PhysicalSourceTypePtr> physicalSources,
                        HardwareManagerPtr&&,
                        std::vector<BufferManagerPtr>&&,
                        QueryManagerPtr&&,
                        std::function<Network::NetworkManagerPtr(std::shared_ptr<NodeEngine>)>&&,
                        Network::PartitionManagerPtr&&,
                        QueryCompilation::QueryCompilerPtr&&,
                        std::weak_ptr<AbstractQueryStatusListener>&&,
                        OpenCLManagerPtr&&,
                        WorkerId nodeEngineId,
                        uint64_t numberOfBuffersInGlobalBufferManager,
                        uint64_t numberOfBuffersInSourceLocalBufferPool,
                        uint64_t numberOfBuffersPerWorker,
                        bool sourceSharing);

  private:
    WorkerId nodeId;
    std::vector<PhysicalSourceTypePtr> physicalSources;
    std::map<SharedQueryId, std::vector<DecomposedQueryId>> sharedQueryIdToDecomposedQueryPlanIds;
    std::map<DecomposedQueryId, Execution::ExecutableQueryPlanPtr> deployedExecutableQueryPlans;
    HardwareManagerPtr hardwareManager;
    std::vector<BufferManagerPtr> bufferManagers;
    QueryManagerPtr queryManager;
    BufferStoragePtr bufferStorage;
    Monitoring::MetricStorePtr metricStore;
    QueryCompilation::QueryCompilerPtr queryCompiler;
    Network::PartitionManagerPtr partitionManager;
    AbstractQueryStatusListenerPtr nesWorker;
    Network::NetworkManagerPtr networkManager;
    Statistic::StatisticManagerPtr statisticManager;
    OpenCLManagerPtr openCLManager;
    std::atomic<bool> isRunning{};
    mutable std::recursive_mutex engineMutex;
    [[maybe_unused]] WorkerId nodeEngineId;
    [[maybe_unused]] uint32_t numberOfBuffersInGlobalBufferManager;
    [[maybe_unused]] uint32_t numberOfBuffersInSourceLocalBufferPool;
    [[maybe_unused]] uint32_t numberOfBuffersPerWorker;
    bool sourceSharing;
};

using NodeEnginePtr = std::shared_ptr<NodeEngine>;

}// namespace Runtime
}// namespace NES
#endif// NES_RUNTIME_INCLUDE_RUNTIME_NODEENGINE_HPP_
