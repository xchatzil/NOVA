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

#include <Network/NetworkSource.hpp>
#include <Network/PartitionManager.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Runtime/Events.hpp>
#include <Util/Logger/Logger.hpp>

// Note that we updated the PartitionManager's logic:
// currently, we do not delete partitions even if their counter is set to 0
// in the past, we deleted that
// rationale: we need a placeholder to keep track of information regarding partitions that were present on a node

namespace NES::Network {

PartitionManager::PartitionConsumerEntry::PartitionConsumerEntry(NodeLocation&& senderLocation, DataEmitterPtr&& emitter)
    : senderLocation(std::move(senderLocation)), consumer(std::move(emitter)) {
    auto networkSource = std::dynamic_pointer_cast<Network::NetworkSource>(consumer);
}

uint64_t PartitionManager::PartitionConsumerEntry::count() const { return partitionCounter; }

void PartitionManager::PartitionConsumerEntry::pin() { partitionCounter++; }

void PartitionManager::PartitionConsumerEntry::unpin() { partitionCounter--; }

DataEmitterPtr PartitionManager::PartitionConsumerEntry::getConsumer() { return consumer; }

DecomposedQueryPlanVersion PartitionManager::PartitionConsumerEntry::getVersion() { return consumer->getVersion(); }

PartitionManager::PartitionProducerEntry::PartitionProducerEntry(NodeLocation&& senderLocation)
    : receiverLocation(std::move(senderLocation)) {
    // nop
}

uint64_t PartitionManager::PartitionProducerEntry::count() const { return partitionCounter; }

void PartitionManager::PartitionProducerEntry::pin() { partitionCounter++; }

void PartitionManager::PartitionProducerEntry::unpin() { partitionCounter--; }

void PartitionManager::PartitionProducerEntry::registerEventListener(Runtime::RuntimeEventListenerPtr eventListener) {
    this->eventListener = eventListener;
}

Runtime::RuntimeEventListenerPtr PartitionManager::PartitionProducerEntry::getEventListener() const { return eventListener; }

void PartitionManager::pinSubpartitionConsumer(NesPartition partition) {
    std::unique_lock lock(consumerPartitionsMutex);
    auto it = consumerPartitions.find(partition);
    if (it != consumerPartitions.end()) {
        it->second.pin();
        return;
    }
    NES_ASSERT2_FMT(false, "Cannot increment partition counter as partition does not exists " << partition);
}

bool PartitionManager::registerSubpartitionConsumer(NesPartition partition,
                                                    NodeLocation senderLocation,
                                                    DataEmitterPtr emitterPtr) {
    std::unique_lock lock(consumerPartitionsMutex);
    //check if partition is present
    auto it = consumerPartitions.find(partition);
    if (it != consumerPartitions.end()) {
        // partition is contained
        it->second.pin();
    } else {
        it = consumerPartitions.insert_or_assign(it,
                                                 partition,
                                                 PartitionConsumerEntry(std::move(senderLocation), std::move(emitterPtr)));
    }
    NES_DEBUG("PartitionManager: Registering Subpartition Consumer {}={}", partition.toString(), (*it).second.count());
    return (*it).second.count() == 1;
}

bool PartitionManager::unregisterSubpartitionConsumer(NesPartition partition) {
    std::unique_lock lock(consumerPartitionsMutex);

    auto it = consumerPartitions.find(partition);
    NES_ASSERT2_FMT(it != consumerPartitions.end(),
                    "PartitionManager: error while unregistering partition " << partition << " reason: partition not found");

    // safeguard
    if (it->second.count() == 0) {
        NES_DEBUG("PartitionManager: Partition {}, counter is at 0.", partition.toString());
        return true;
    }

    it->second.unpin();

    NES_INFO("PartitionManager: Unregistering Consumer {}; newCnt({})", partition.toString(), it->second.count());
    if (it->second.count() == 1) {
        NES_DEBUG("PartitionManager: Consumer {}, counter is at 1.", partition.toString());
        return true;
    }
    return false;
}

std::optional<uint64_t> PartitionManager::getSubpartitionConsumerCounter(NesPartition partition) {
    std::unique_lock lock(consumerPartitionsMutex);
    if (auto it = consumerPartitions.find(partition); it != consumerPartitions.end()) {
        return it->second.count();
    }
    return std::nullopt;
}

DecomposedQueryPlanVersion PartitionManager::getVersion(NesPartition partition) {
    std::unique_lock lock(consumerPartitionsMutex);
    if (auto it = consumerPartitions.find(partition); it != consumerPartitions.end()) {
        return it->second.getVersion();
    }
    NES_ASSERT(false, "Trying to check version of non-existent partition");
    return false;
}

DataEmitterPtr PartitionManager::getDataEmitter(NesPartition partition) {
    std::unique_lock lock(consumerPartitionsMutex);
    if (auto it = consumerPartitions.find(partition); it != consumerPartitions.end()) {
        return it->second.getConsumer();
    }
    return nullptr;
}

Runtime::RuntimeEventListenerPtr PartitionManager::getEventListener(NesPartition partition) const {
    std::unique_lock lock(producerPartitionsMutex);
    if (auto it = producerPartitions.find(partition); it != producerPartitions.end()) {
        return it->second.getEventListener();
    }
    return nullptr;
}

void PartitionManager::clear() {
    std::scoped_lock lock(producerPartitionsMutex, consumerPartitionsMutex);
    NES_DEBUG("PartitionManager: Running sanity check on partitions with refCnt > 0");
    for (auto&& [partition, metadata] : producerPartitions) {
        NES_ASSERT2_FMT(metadata.count() == 0,
                        "PartitionManager: Producer Partition " << partition << " is still alive: " << metadata.count());
    }
    for (auto&& [partition, metadata] : consumerPartitions) {
        NES_ASSERT2_FMT(metadata.count() == 0,
                        "PartitionManager: Consumer Partition " << partition << " is still alive: " << metadata.count());
    }
    NES_DEBUG("PartitionManager: Clearing registered partitions");
    producerPartitions.clear();
    consumerPartitions.clear();
}

PartitionRegistrationStatus PartitionManager::getConsumerRegistrationStatus(NesPartition partition) const {
    //check if partition is present
    std::unique_lock lock(consumerPartitionsMutex);
    if (auto it = consumerPartitions.find(partition); it != consumerPartitions.end()) {
        return it->second.count() > 0 ? PartitionRegistrationStatus::Registered : PartitionRegistrationStatus::Deleted;
    }
    return PartitionRegistrationStatus::NotFound;
}

PartitionRegistrationStatus PartitionManager::getProducerRegistrationStatus(NesPartition partition) const {
    //check if partition is present
    std::unique_lock lock(producerPartitionsMutex);
    if (auto it = producerPartitions.find(partition); it != producerPartitions.end()) {
        return it->second.count() > 0 ? PartitionRegistrationStatus::Registered : PartitionRegistrationStatus::Deleted;
    }
    return PartitionRegistrationStatus::NotFound;
}

bool PartitionManager::registerSubpartitionProducer(NesPartition partition, NodeLocation receiverLocation) {
    std::unique_lock lock(producerPartitionsMutex);
    //check if partition is present
    auto it = producerPartitions.find(partition);
    if (it != producerPartitions.end()) {
        // partition is contained
        it->second.pin();
    } else {
        it = producerPartitions.insert_or_assign(it, partition, PartitionProducerEntry(std::move(receiverLocation)));
    }
    NES_DEBUG("PartitionManager: Registering Subpartition Producer {}={}", partition.toString(), (*it).second.count());
    return (*it).second.count() == 1;// first time
}

bool PartitionManager::addSubpartitionEventListener(NesPartition partition,
                                                    NodeLocation receiverLocation,
                                                    Runtime::RuntimeEventListenerPtr eventListener) {
    std::unique_lock lock(producerPartitionsMutex);
    //check if partition is present
    if (auto it = producerPartitions.find(partition); it == producerPartitions.end()) {
        it = producerPartitions.insert_or_assign(it, partition, PartitionProducerEntry(std::move(receiverLocation)));
        it->second.registerEventListener(eventListener);
        NES_DEBUG("PartitionManager: Registering Subpartition Event Consumer {}={}", partition.toString(), (*it).second.count());
        return true;
    }
    NES_DEBUG("PartitionManager: Cannot register {}", partition.toString());
    return false;
}

bool PartitionManager::unregisterSubpartitionProducer(NesPartition partition) {
    std::unique_lock lock(producerPartitionsMutex);

    auto it = producerPartitions.find(partition);
    NES_ASSERT2_FMT(it != producerPartitions.end(),
                    "PartitionManager: error while unregistering partition " << partition << " reason: partition not found");

    // safeguard
    if (it->second.count() == 0) {
        NES_DEBUG("PartitionManager: Partition {}, counter is at 0.", partition.toString());
        return true;
    }

    it->second.unpin();

    NES_INFO("PartitionManager: Unregistering Subpartition Producer {}; newCnt({})", partition.toString(), it->second.count());
    if (it->second.count() == 0) {
        NES_DEBUG("PartitionManager: Producer Partition {}, counter is at 0.", partition.toString());
        return true;
    }
    return false;
}

std::optional<uint64_t> PartitionManager::getSubpartitionProducerCounter(NesPartition partition) {
    std::unique_lock lock(producerPartitionsMutex);
    if (auto it = producerPartitions.find(partition); it != producerPartitions.end()) {
        return it->second.count();
    }
    return std::nullopt;
}

void PartitionManager::pinSubpartitionProducer(NesPartition partition) {
    std::unique_lock lock(producerPartitionsMutex);
    if (auto it = producerPartitions.find(partition); it != producerPartitions.end()) {
        it->second.pin();
    }
}

PartitionManager::~PartitionManager() { clear(); }
}// namespace NES::Network
