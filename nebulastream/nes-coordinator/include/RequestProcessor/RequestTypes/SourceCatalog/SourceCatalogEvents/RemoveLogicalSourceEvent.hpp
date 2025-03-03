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

#ifndef NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_SOURCECATALOG_SOURCECATALOGEVENTS_REMOVELOGICALSOURCEEVENT_HPP_
#define NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_SOURCECATALOG_SOURCECATALOGEVENTS_REMOVELOGICALSOURCEEVENT_HPP_

#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/BaseUpdateSourceCatalogEvent.hpp>

namespace NES::RequestProcessor {
class RemoveLogicalSourceEvent;
using RemoveLogicalSourceEventPtr = std::shared_ptr<RemoveLogicalSourceEvent>;

/**
 * @brief An event to remove a logical source from the source catalog
 */
class RemoveLogicalSourceEvent : public BaseUpdateSourceCatalogEvent {
  public:
    /**
     * @brief Create a new event
     * @param logicalSourceName The name of the logical source to remove
     * @return a pointer to the new event
     */
    static RemoveLogicalSourceEventPtr create(std::string logicalSourceName);

    /**
     * @brief Constructor
     * @param logicalSourceName The name of the logical source to remove
     */
    RemoveLogicalSourceEvent(std::string logicalSourceName);

    /**
     * @brief Get the logical source name
     * @return a string representing the logical source name
     */
    std::string getLogicalSourceName() const;

  private:
    std::string logicalSourceName;
};
}// namespace NES::RequestProcessor
#endif// NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_SOURCECATALOG_SOURCECATALOGEVENTS_REMOVELOGICALSOURCEEVENT_HPP_
