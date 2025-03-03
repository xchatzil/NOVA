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

#ifdef ENABLE_OPC_BUILD

#include <Sources/OPCSource.hpp>
#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/client_subscriptions.h>
#include <open62541/plugin/log_stdout.h>

#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>

#include <Runtime/BufferManager.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/QueryManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <open62541/types.h>
#include <utility>

namespace NES {

OPCSource::OPCSource(const SchemaPtr& schema,
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
                     std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors)
    : DataSource(schema,
                 std::move(bufferManager),
                 std::move(queryManager),
                 operatorId,
                 originId,
                 statisticId,
                 numSourceLocalBuffers,
                 gatheringMode,
                 physicalSourceName,
                 false,
                 std::move(executableSuccessors)),
      connected(false), url(url), nodeId(nodeId), user(std::move(std::move(user))), password(std::move(password)),
      retval(UA_STATUSCODE_GOOD), client(UA_Client_new()) {

    NES_DEBUG("OPCSOURCE {} : Init OPC Source to  {}  with user and password.", this->toString(), url);
}

OPCSource::~OPCSource() {
    NES_DEBUG("OPCSource::~OPCSource()");
    bool success = disconnect();
    if (success) {
        NES_DEBUG("OPCSOURCE {}: Destroy OPC Source", this->toString());
    } else {
        NES_FATAL_ERROR("OPCSOURCE {} : Destroy OPC Source failed cause it could not be disconnected", this->toString());
    }
}

std::optional<Runtime::TupleBuffer> OPCSource::receiveData() {

    NES_DEBUG("OPCSOURCE::receiveData() {}: receiveData() ", this->toString());
    if (connect()) {

        auto* val = new UA_Variant;
        retval = UA_Client_readValueAttribute(client, nodeId, val);
        auto buffer = bufferManager->getBufferBlocking();
        buffer.setNumberOfTuples(1);
        NES_DEBUG("OPCSOURCE::receiveData() {}: got buffer ", this->toString());

        if (retval == UA_STATUSCODE_GOOD && UA_Variant_isScalar(val)) {
            NES_DEBUG("OPCSOURCE::receiveData() Value datatype is: {}", val->type->typeName);
            std::memcpy(buffer.getBuffer(), val->data, val->type->memSize);
            UA_delete(val, val->type);
            return buffer;
        } else {
            UA_delete(val, val->type);
            NES_ERROR("OPCSOURCE::receiveData() error: Could not retrieve data. Further inspection needed.");
            return std::nullopt;
        }

    } else {
        NES_ERROR("OPCSOURCE::receiveData(): Not connected!");
        return std::nullopt;
    }
}

std::string OPCSource::toString() const {

    char* ident = (char*) UA_malloc(sizeof(char) * nodeId.identifier.string.length + 1);
    memcpy(ident, nodeId.identifier.string.data, nodeId.identifier.string.length);
    ident[nodeId.identifier.string.length] = '\0';

    std::stringstream ss;
    ss << "OPC_SOURCE(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    ss << "URL= " << url << ", ";
    ss << "NODE_INDEX= " << nodeId.namespaceIndex << ", ";
    ss << "NODE_IDENTIFIER= " << ident << ". ";

    return ss.str();
}

bool OPCSource::connect() {

    UA_ClientConfig_setDefault(UA_Client_getConfig(client));

    if (!connected) {

        NES_DEBUG("OPCSOURCE::connect(): was !conncect now connect {}: connected", this->toString());
        retval = UA_Client_connect(client, url.c_str());
        NES_DEBUG("OPCSOURCE::connect(): connected without user or password");
        NES_DEBUG("OPCSOURCE::connect(): use address {}", url);

        if (retval != UA_STATUSCODE_GOOD) {

            UA_Client_delete(client);
            connected = false;
            NES_ERROR("OPCSOURCE::connect(): ERROR with Status Code: {} OPCSOURCE {}: set connected false",
                      retval,
                      this->toString());
        } else {

            connected = true;
        }
    }

    if (connected) {
        NES_DEBUG("OPCSOURCE::connect():  {}: connected", this->toString());
    } else {
        NES_DEBUG("Exception: OPCSOURCE::connect():  {}: NOT connected", this->toString());
    }
    return connected;
}

bool OPCSource::disconnect() {
    NES_DEBUG("OPCSource::disconnect() connected={}", connected);
    if (connected) {

        NES_DEBUG("OPCSOURCE::disconnect() disconnect client");
        UA_Client_disconnect(client);
        NES_DEBUG("OPCSOURCE::disconnect() delete client");
        UA_Client_delete(client);
        connected = false;
    }
    if (!connected) {
        NES_DEBUG("OPCSOURCE::disconnect()  {}: disconnected", this->toString());
    } else {
        NES_DEBUG("OPCSOURCE::disconnect()  {}: NOT disconnected", this->toString());
    }
    return !connected;
}

SourceType OPCSource::getType() const { return SourceType::OPC_SOURCE; }

std::string OPCSource::getUrl() const { return url; }

UA_NodeId OPCSource::getNodeId() const { return nodeId; }

std::string OPCSource::getUser() const { return user; }

std::string OPCSource::getPassword() const { return password; }

}// namespace NES
#endif
