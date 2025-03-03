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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_HARDWAREMANAGER_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_HARDWAREMANAGER_HPP_

#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/Allocator/NumaRegionMemoryAllocator.hpp>
#include <map>
#include <memory>
#include <unordered_map>
#include <vector>

namespace NES::Runtime {

/**
 * @brief This class is responsible to look up OS/HW specs of the underlying hardware, e.g., numa.
 */
class HardwareManager {

  public:
    class NumaDescriptor;
    /**
     * @brief Descriptor for a single core
     */
    struct CpuDescriptor {
        friend class NumaDescriptor;

      public:
        explicit CpuDescriptor(uint16_t coreId = -1, uint16_t cpuId = -1) : coreId(coreId), cpuId(cpuId) {
            // nop
        }

        /**
         * @brief Provide the logical core id
         * @return
         */
        uint16_t getCoreId() const { return coreId; }

        /**
         * @brief Provides the CPU id assigned to a core
         * @return
         */
        uint16_t getCpuId() const { return cpuId; }

        friend bool operator<(const CpuDescriptor& lhs, const CpuDescriptor& rhs) { return lhs.cpuId < rhs.cpuId; }

        friend bool operator==(const CpuDescriptor& lhs, const CpuDescriptor& rhs) { return lhs.cpuId == rhs.cpuId; }

      private:
        uint16_t coreId = -1;
        uint16_t cpuId = -1;
    };

    /**
     * @brief Descriptor for a single numa node
     */
    class NumaDescriptor {
      public:
        explicit NumaDescriptor(uint32_t node_id = -1) : nodeId(node_id), physicalCpus() {
            // nop
        }

        NumaDescriptor(const NumaDescriptor& other) { *this = other; }

        NumaDescriptor& operator=(const NumaDescriptor& other) {
            nodeId = other.nodeId;
            physicalCpus = std::map<uint16_t, CpuDescriptor>(other.physicalCpus.begin(), other.physicalCpus.end());
            return *this;
        }

        /**
         * @brief Adds a new cpu/code
         * @param cpu the cpu index
         * @param core the core index
         */
        void addCpu(uint16_t cpu, uint16_t core) {
            if (physicalCpus.find(core) == physicalCpus.end()) {
                physicalCpus[core] = CpuDescriptor(core, cpu);
            } else {
                physicalCpus[core].cpuId = std::min<uint16_t>(physicalCpus[core].cpuId, cpu);
            }
        }

        uint32_t getNodeId() const { return nodeId; }

      private:
        uint32_t nodeId;
        std::map<uint16_t, CpuDescriptor> physicalCpus;
    };

  public:
    /**
     * @brief Creates a new HW manager with a mapping of the CPU/Mem topology
     */
    explicit HardwareManager();

    /**
     * @brief Returns the global posix-based memory allocator
     * @return
     */
    NesDefaultMemoryAllocatorPtr getGlobalAllocator() const;

    /**
     * @brief Binds the given pthread to a specific core of a given numa region
     * @param thread the pthread handle
     * @param numaIndex the numa index
     * @param coreId the core id
     * @return true if was successful
     */
    bool bindThreadToCore(pthread_t thread, uint32_t numaIndex, uint32_t coreId);

#ifdef NES_USE_ONE_QUEUE_PER_NUMA_NODE
    /**
     * @brief Returns the numa allocator for the numaNodeIndex-th numa node
     * @param numaNodeIndex
     * @return
     */
    NumaRegionMemoryAllocatorPtr getNumaAllocator(uint32_t numaNodeIndex) const;
#endif

    /**
     * @brief Returns the numa region for a particular core
     * @return numa region
     */
    uint32_t getNumaNodeForCore(int coreId) const;

  private:
    NesDefaultMemoryAllocatorPtr globalAllocator;
#ifdef NES_USE_ONE_QUEUE_PER_NUMA_NODE
    std::vector<NumaRegionMemoryAllocatorPtr> numaRegions;
#endif
    std::unordered_map<uint64_t, NumaDescriptor> cpuMapping;
    /// number of numa regions
    uint32_t numaNodesCount = 0;
    /// number of physical cores
    uint32_t numPhysicalCpus = 0;
};

}// namespace NES::Runtime

#endif// NES_RUNTIME_INCLUDE_RUNTIME_HARDWAREMANAGER_HPP_
