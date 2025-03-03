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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_RECONFIGURABLE_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_RECONFIGURABLE_HPP_

#include <Runtime/ReconfigurationType.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Util/VirtualEnableSharedFromThis.hpp>

namespace NES::Runtime {
class ReconfigurationMessage;
/**
* @brief Nes components that require to be reconfigured at Runtime need to
* inherit from this class. It provides a reconfigure callback that will be called
* per thread and a postReconfigurationCallback that will be called on the last thread the executes
* the reconfiguration.
*/
class Reconfigurable : public NES::detail::virtual_enable_shared_from_this<Reconfigurable, false> {
  public:
    ~Reconfigurable() NES_NOEXCEPT(false) override = default;

    /**
     * @brief reconfigure callback that will be called per thread
    */
    virtual void reconfigure(ReconfigurationMessage&, WorkerContext&) {
        // nop
    }

    /**
     * @brief callback that will be called on the last thread the executes
     * the reconfiguration
   */
    virtual void postReconfigurationCallback(ReconfigurationMessage&) {
        // nop
    }
};

}// namespace NES::Runtime

#endif// NES_RUNTIME_INCLUDE_RUNTIME_RECONFIGURABLE_HPP_
