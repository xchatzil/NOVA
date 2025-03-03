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

#ifndef NES_RUNTIME_INCLUDE_SOURCES_OPCSOURCE_HPP_
#define NES_RUNTIME_INCLUDE_SOURCES_OPCSOURCE_HPP_
#ifdef ENABLE_OPC_BUILD

#include <Sources/DataSource.hpp>
#include <cstdint>
#include <memory>
#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/plugin/log_stdout.h>
#include <string>

namespace NES {
class TupleBuffer;

/**
 * @brief this class provide a zmq as data source
 */
class OPCSource : public DataSource {

  public:
    /**
     * @brief constructor for the opc source
     * @param schema schema of the elements
     * @param bufferManager pointer to the buffer manager
     * @param queryManager pointer to the query manager
     * @param url the url of the OPC server
     * @param nodeId the node id of the desired node
     * @param password for authentication if needed
     * @param user name if connecting with a server with authentication
     * @param operatorId current operator id
     * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
     * @param statisticId represents the unique identifier of components that we can track statistics for
     * @param numSourceLocalBuffers the number of buffers allocated to a source
     * @param gatheringMode the gathering mode (INTERVAL_MODE, INGESTION_RATE_MODE, or ADAPTIVE_MODE)
     * @param physicalSourceName the name and unique identifier of a physical source
     * @param executableSuccessors the subsequent operators in the pipeline to which the data is pushed
     */
    explicit OPCSource(const SchemaPtr& schema,
                       Runtime::BufferManagerPtr bufferManager,
                       Runtime::QueryManagerPtr queryManager,
                       const std::string& url,
                       UA_NodeId nodeId,
                       std::string password,
                       std::string user,
                       OperatorId operatorId,
                       OriginId originId,
                       StatisticId statisticId,
                       size_t numSourceLocalBuffers,
                       GatheringMode gatheringMode,
                       const std::string& physicalSourceName,
                       std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors);

    /**
     * @brief destructor of OPC source that disconnects the queue before deconstruction
     * @note if queue cannot be disconnected, an assertion is raised
     */
    ~OPCSource() override;

    /**
     * @brief blocking method to receive a buffer from the OPC source
     * @return TupleBufferPtr containing the received buffer
     */
    std::optional<Runtime::TupleBuffer> receiveData() override;

    /**
     * @brief override the toString method for the opc source
     * @return returns string describing the opc source
     */
    std::string toString() const override;

    /**
     * @brief The url of the OPC server
     * @return OPC url
     */
    std::string getUrl() const;

    /**
     * @brief get desired OPC node id
     * @return OPC node id
     */
    UA_NodeId getNodeId() const;

    /**
     * @brief get user name for OPC server
     * @return opc server user name
     */
    std::string getUser() const;

    /**
     * @brief get password for OPC server
     * @return opc server password
     */
    std::string getPassword() const;

    /**
     * @brief Get source type
     */
    SourceType getType() const override;

  private:
    /**
     * @brief method to connect opc using the url specified before
     * check if already connected, if not connect try to connect, if already connected return
     * @return bool indicating if connection could be established
     */
    bool connect();

    /**
     * @brief method to disconnect opc using the url specified before
     * check if connected, if connected try to disconnect, if not connected return
     * @return bool indicating if connection could be disconnected
     */
    bool disconnect();

    /**
     * @brief method for serialization, all listed variable below are added to the
     * serialization/deserialization process
     */
    friend class DataSource;

  private:
    bool connected;
    const std::string url;
    UA_NodeId nodeId;
    const std::string user;
    const std::string password;
    UA_StatusCode retval;
    UA_Client* client;
};

using OPCSourcePtr = std::shared_ptr<OPCSource>;
}// namespace NES

#endif
#endif// NES_RUNTIME_INCLUDE_SOURCES_OPCSOURCE_HPP_
