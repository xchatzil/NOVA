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

#ifdef ENABLE_MQTT_BUILD

#include <API/AttributeField.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Operators/LogicalOperators/Sources/MQTTSourceDescriptor.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/MQTTSource.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Sources/Parsers/JSONParser.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <mqtt/async_client.h>
#include <sstream>
#include <string>
#include <utility>

using namespace std;
using namespace std::chrono;

namespace NES {
/*
 the user can specify the time unit for the delay and the duration of the delay in that time unit
 in order to avoid type switching types (different time units require different duration types), the user input for
 the duration is treated as nanoseconds and then multiplied to 'convert' to milliseconds or seconds accordingly
*/

MQTTSource::MQTTSource(SchemaPtr schema,
                       Runtime::BufferManagerPtr bufferManager,
                       Runtime::QueryManagerPtr queryManager,
                       const MQTTSourceTypePtr& sourceConfig,
                       OperatorId operatorId,
                       OriginId originId,
                       StatisticId statisticId,
                       size_t numSourceLocalBuffers,
                       GatheringMode gatheringMode,
                       const std::string& physicalSourceName,
                       std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors)
    : DataSource(schema,
                 bufferManager,
                 queryManager,
                 operatorId,
                 originId,
                 statisticId,
                 numSourceLocalBuffers,
                 gatheringMode,
                 physicalSourceName,
                 false,
                 std::move(executableSuccessors)),
      sourceConfig(sourceConfig), connected(false), serverAddress(sourceConfig->getUrl()->getValue()),
      clientId(sourceConfig->getClientId()->getValue()), user(sourceConfig->getUserName()->getValue()),
      topic(sourceConfig->getTopic()->getValue()), tupleSize(schema->getSchemaSizeInBytes()),
      qualityOfService(MQTTSourceDescriptor::ServiceQualities(sourceConfig->getQos()->getValue())),
      cleanSession(sourceConfig->getCleanSession()->getValue()),
      bufferFlushIntervalMs(sourceConfig->getFlushIntervalMS()->getValue()),
      readTimeoutInMs(sourceConfig->getFlushIntervalMS()->getValue() > 0 ? sourceConfig->getFlushIntervalMS()->getValue() : 100) {

    //TODO: This is a workaround currently. Every MQTT client needs to connect with a unique ID otherwise MQTT broker will
    // disconnect clients with same client ID while accepting request for a new client with same id. For now we use the combination
    // of logical and physical source name. A better solution will be to have a UUID as the client id.
    this->clientId = this->clientId + "-" + sourceConfig->getLogicalSourceName() + "_" + sourceConfig->getPhysicalSourceName()
        + "_" + operatorId.toString();
    client = std::make_shared<mqtt::async_client>(serverAddress, this->clientId);

    //init physical types
    std::vector<std::string> schemaKeys;
    std::string fieldName;
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();

    //Extracting the schema keys in order to parse incoming data correctly (e.g. use as keys for JSON objects)
    //Also, extracting the field types in order to parse and cast the values of incoming data to the correct types
    for (const auto& field : schema->fields) {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
        fieldName = field->getName();
        schemaKeys.push_back(fieldName.substr(fieldName.find('$') + 1, fieldName.size() - 1));
    }

    switch (sourceConfig->getInputFormat()->getValue()) {
        case Configurations::InputFormat::JSON:
            inputParser = std::make_unique<JSONParser>(schema->getSize(), schemaKeys, physicalTypes);
            break;
        case Configurations::InputFormat::CSV:
            inputParser = std::make_unique<CSVParser>(schema->getSize(), physicalTypes, ",");
            break;
        case Configurations::InputFormat::NES_BINARY: NES_NOT_IMPLEMENTED();
    }

    NES_DEBUG("MQTTSource::MQTTSource: Init MQTTSource to {} with client id: {}.", serverAddress, clientId);
}

MQTTSource::~MQTTSource() {
    NES_DEBUG("MQTTSource::~MQTTSource()");
    bool success = disconnect();
    if (success) {
        NES_DEBUG("MQTTSource::~MQTTSource: Destroy MQTT Source");
    } else {
        NES_ERROR("MQTTSource::~MQTTSource: Destroy MQTT Source failed cause it could not be disconnected");
        assert(0);
    }
    NES_DEBUG("MQTTSource::~MQTTSource: Destroy MQTT Source");
}

std::optional<Runtime::TupleBuffer> MQTTSource::receiveData() {
    NES_DEBUG("MQTTSource: receiveData ");
    auto buffer = allocateBuffer();
    if (connect()) {
        if (!fillBuffer(buffer)) {
            NES_ERROR("MQTTSource::receiveData: Failed to fill the TupleBuffer.");
            return std::nullopt;
        }
    } else {
        NES_ERROR("MQTTSource::receiveData: Not connected!");
        return std::nullopt;
    }
    if (buffer.getNumberOfTuples() == 0) {
        return std::nullopt;
    }
    return buffer.getBuffer();
}

std::string MQTTSource::toString() const {
    std::stringstream ss;
    ss << "MQTTSOURCE(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    ss << "SERVERADDRESS=" << serverAddress << ", ";
    ss << "CLIENTID=" << clientId << ", ";
    ss << "USER=" << user << ", ";
    ss << "TOPIC=" << topic << ", ";
    ss << "DATATYPE=" << magic_enum::enum_name(sourceConfig->getInputFormat()->getValue()) << ", ";
    ss << "QOS=" << magic_enum::enum_name(qualityOfService) << ", ";
    ss << "CLEANSESSION=" << cleanSession << ". ";
    ss << "BUFFERFLUSHINTERVALMS=" << bufferFlushIntervalMs << ". ";
    return ss.str();
}

bool MQTTSource::fillBuffer(Runtime::MemoryLayouts::TestTupleBuffer& tupleBuffer) {

    // determine how many tuples fit into the buffer
    tuplesThisPass = tupleBuffer.getCapacity();
    NES_DEBUG("MQTTSource::fillBuffer: Fill buffer with #tuples= {}  of size= {}", tuplesThisPass, tupleSize);

    uint64_t tupleCount = 0;
    auto flushIntervalTimerStart = std::chrono::system_clock::now();

    bool flushIntervalPassed = false;
    while (tupleCount < tuplesThisPass && !flushIntervalPassed) {
        std::string receivedMessageString;
        try {
            NES_TRACE("Waiting for messages on topic: '{}'", topic);

            // Try to consume a message if the connected flag is set.
            // If no message is received (nullptr) and if the client is not connected anymore, set connected to false.
            // If connected is false, constantly check if we are reconnected again and if so, resubscribe.
            if (connected) {
                // Using try_consume_message_for(), because it is non-blocking.
                auto message = client->try_consume_message_for(std::chrono::milliseconds(readTimeoutInMs));
                if (message) {// Check if message was received correctly (not nullptr)
                    NES_TRACE("Client consume message: '{}'", message->get_payload_str());
                    receivedMessageString = message->get_payload_str();
                    if (!inputParser->writeInputTupleToTupleBuffer(receivedMessageString,
                                                                   tupleCount,
                                                                   tupleBuffer,
                                                                   schema,
                                                                   localBufferManager)) {
                        NES_ERROR("MQTTSource::getBuffer: Failed to write input tuple to TupleBuffer.");
                        return false;
                    }
                    NES_DEBUG("MQTTSource::fillBuffer: Tuples processed for current buffer: {} / {}", tupleCount, tuplesThisPass);
                    tupleCount++;
                } else if (!client->is_connected()) {// message is a nullptr. Check if still connected to broker.
                    NES_WARNING("MQTTSource::fillBuffer: Not connected anymore!");
                    connected = false;
                }
            } else if (client->is_connected()) {// We lost connection (connection=false), check if we are connected again.
                NES_DEBUG("MQTTSource::fillBuffer: Reconnected, subscribing again!");
                client->subscribe(topic, magic_enum::enum_integer(qualityOfService))->wait_for(readTimeoutInMs);
                connected = true;
            }
        } catch (...) {
            auto exceptionPtr = std::current_exception();
            try {
                if (exceptionPtr) {
                    std::rethrow_exception(exceptionPtr);
                }
            } catch (const mqtt::exception& error) {
                NES_ERROR("MQTTSource::fillBuffer: {}", error.what());
                return false;
            } catch (std::exception& error) {
                NES_ERROR("MQTTSource::fillBuffer: General Error: {}", error.what());
                return false;
            }
        }

        // If bufferFlushIntervalMs was defined by the user (> 0), we check whether the time on receiving
        // and writing data exceeds the user defined limit (bufferFlushIntervalMs).
        // If so, we flush the current TupleBuffer(TB) and proceed with the next TB.
        if ((bufferFlushIntervalMs > 0 && tupleCount > 0
             && std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - flushIntervalTimerStart)
                     .count()
                 >= bufferFlushIntervalMs)) {
            NES_DEBUG("MQTTSource::fillBuffer: Reached TupleBuffer flush interval. Finishing writing to current TupleBuffer.");
            flushIntervalPassed = true;
        }
    }//end of while
    tupleBuffer.setNumberOfTuples(tupleCount);
    generatedTuples += tupleCount;
    generatedBuffers++;
    return true;
}

bool MQTTSource::connect() {
    if (!connected) {
        NES_DEBUG("MQTTSource was !connect now connect: connected");
        // connect with username and password
        try {
            //automatic reconnect = true enables establishing a connection with a broker again, after a disconnect
            auto connOpts =
                mqtt::connect_options_builder().user_name(user).automatic_reconnect(true).clean_session(cleanSession).finalize();

            // Start consumer before connecting to make sure to not miss messages
            client->start_consuming();

            // Connect to the server
            NES_DEBUG("MQTTSource::connect Connecting to the MQTT server...");
            auto tok = client->connect(connOpts);

            // Getting the connect response will block waiting for the
            // connection to complete.
            auto rsp = tok->get_connect_response();

            // If there is no session present, then we need to subscribe, but if
            // there is a session, then the server remembers us and our
            // subscriptions.
            if (!rsp.is_session_present()) {
                client->subscribe(topic, magic_enum::enum_integer(qualityOfService))->wait();
            }
            connected = client->is_connected();
        } catch (const mqtt::exception& error) {
            NES_WARNING("MQTTSource::connect: {}", error.to_string());
            connected = false;
            return connected;
        }

        if (connected) {
            NES_DEBUG("MQTTSource::connect: Connection established with topic: {}", topic);
            NES_DEBUG("MQTTSource::connect: connected");
        } else {
            NES_DEBUG("MQTTSource::connect: NOT connected");
        }
    }
    return connected;
}

bool MQTTSource::disconnect() {
    NES_DEBUG("MQTTSource::disconnect connected={}", connected);
    if (connected) {
        // If we're here, the client was almost certainly disconnected.
        // But we check, just to make sure.
        if (client->is_connected()) {
            NES_DEBUG("MQTTSource: Shutting down and disconnecting from the MQTT server.");
            // In a non-clean(persistent) session expects, the broker expects the client to stay subscribed to the topic
            // -> even unsubscribing and resubscribing does not work, the (only?) way to stop a non-clean(persistent)
            // -> session is to establish a clean session using the SAME clientID (as was used for the non-clean session)
            if (cleanSession) {
                client->unsubscribe(topic)->wait();
            }
            client->disconnect()->wait();
            NES_DEBUG("MQTTSource::disconnect: disconnected.");
        } else {
            NES_DEBUG("MQTTSource::disconnect: Client was already disconnected");
        }
        connected = client->is_connected();
    }
    if (!connected) {
        NES_DEBUG("MQTTSource::disconnect: disconnected");
    } else {
        NES_DEBUG("MQTTSource::disconnect: NOT disconnected");
        return connected;
    }
    return !connected;
}
const string& MQTTSource::getServerAddress() const { return serverAddress; }

const string& MQTTSource::getClientId() const { return clientId; }

const string& MQTTSource::getUser() const { return user; }

const string& MQTTSource::getTopic() const { return topic; }

uint64_t MQTTSource::getTupleSize() const { return tupleSize; }

SourceType MQTTSource::getType() const { return SourceType::MQTT_SOURCE; }

MQTTSourceDescriptor::ServiceQualities MQTTSource::getQualityOfService() const { return qualityOfService; }

uint64_t MQTTSource::getTuplesThisPass() const { return tuplesThisPass; }

bool MQTTSource::getCleanSession() const { return cleanSession; }

std::vector<PhysicalTypePtr> MQTTSource::getPhysicalTypes() const { return physicalTypes; }

const MQTTSourceTypePtr& MQTTSource::getSourceConfigPtr() const { return sourceConfig; }

}// namespace NES
#endif
