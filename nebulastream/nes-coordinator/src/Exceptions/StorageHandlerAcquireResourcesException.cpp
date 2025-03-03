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
#include <Exceptions/StorageHandlerAcquireResourcesException.hpp>
#include <utility>
namespace NES::Exceptions {
/**
 * This request is raised when an error occurs during resource acquisition by the storage handler and the error is not related to
 * the resource being locked already. Trying to lock a resource which has been locked before will instead raise a ResourceLockingException
 * @param message a message indicating what went wrong
 */
StorageHandlerAcquireResourcesException::StorageHandlerAcquireResourcesException(const std::string& message)
    : RequestExecutionException(message) {}
}// namespace NES::Exceptions
