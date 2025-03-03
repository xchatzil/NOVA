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
#ifndef NES_COORDINATOR_INCLUDE_REST_CONTROLLER_TOPOLOGYCONTROLLER_HPP_
#define NES_COORDINATOR_INCLUDE_REST_CONTROLLER_TOPOLOGYCONTROLLER_HPP_

#include <Catalogs/Topology/Topology.hpp>
#include <Identifiers/NESStrongTypeJson.hpp>
#include <REST/Controller/BaseRouterPrefix.hpp>
#include <REST/Handlers/ErrorHandler.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <Util/Mobility/Waypoint.hpp>
#include <nlohmann/json.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <utility>

#include OATPP_CODEGEN_BEGIN(ApiController)

namespace NES {
class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class ErrorHandler;
using ErrorHandlerPtr = std::shared_ptr<ErrorHandler>;

namespace REST::Controller {
class TopologyController : public oatpp::web::server::api::ApiController {

  public:
    /**
     * Constructor with object mapper.
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     * @param topology - the overall physical infrastructure with different nodes
     * @param completeRouterPrefix - url consisting of base router prefix (e.g "v1/nes/") and controller specific router prefix (e.g "connectivityController")
     * @param errorHandler - responsible for handling errors
     */
    TopologyController(const std::shared_ptr<ObjectMapper>& objectMapper,
                       const TopologyPtr& topology,
                       const oatpp::String& completeRouterPrefix,
                       const ErrorHandlerPtr& errorHandler)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix), topology(topology),
          errorHandler(errorHandler) {}

    /**
     * Create a shared object of the API controller
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     * @param topology - the overall physical infrastructure with different nodes
     * @param routerPrefixAddition - controller specific router prefix (e.g "connectivityController/")
     * @param errorHandler - responsible for handling errors
     */
    static std::shared_ptr<TopologyController> create(const std::shared_ptr<ObjectMapper>& objectMapper,
                                                      const TopologyPtr& topology,
                                                      const std::string& routerPrefixAddition,
                                                      const ErrorHandlerPtr& errorHandler) {
        oatpp::String completeRouterPrefix = BASE_ROUTER_PREFIX + routerPrefixAddition;
        return std::make_shared<TopologyController>(objectMapper, std::move(topology), completeRouterPrefix, errorHandler);
    }

    ENDPOINT("GET", "", getTopology) {
        try {
            auto topologyJson = topology->toJson();
            return createResponse(Status::CODE_200, topologyJson.dump());
        } catch (nlohmann::json::exception e) {
            return errorHandler->handleError(Status::CODE_500, e.what());
        } catch (const std::exception& exc) {
            NES_ERROR("TopologyController: handleGet -getTopology: Exception occurred while building the "
                      "topology: {}",
                      exc.what());
            return errorHandler->handleError(Status::CODE_500,
                                             "Exception occurred while building topology" + std::string(exc.what()));
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }

    ENDPOINT("POST", "/addAsChild", addParent, BODY_STRING(String, request)) {
        try {
            std::string req = request.getValue("{}");
            //check if json is valid
            if (!nlohmann::json::accept(req)) {
                return errorHandler->handleError(Status::CODE_400, "Invalid JSON");
            };
            nlohmann::json reqJson = nlohmann::json::parse(req);
            auto optional = validateRequest(reqJson);
            if (optional.has_value()) {
                return optional.value();
            }

            auto parentId = reqJson["parentId"].get<WorkerId>();
            auto childId = reqJson["childId"].get<WorkerId>();

            auto children = topology->getChildTopologyNodeIds(parentId);
            for (const auto& child : children) {
                if (child == childId) {
                    return errorHandler->handleError(Status::CODE_400,
                                                     fmt::format("Could not add parent for node in topology: Node with "
                                                                 "childId={} is already a child of node with parentID={}.",
                                                                 childId,
                                                                 parentId));
                }
            }
            bool added = topology->addTopologyNodeAsChild(parentId, childId);
            if (added) {
                NES_DEBUG("TopologyController::handlePost:addParent: created link successfully new topology is=");
            } else {
                NES_ERROR("TopologyController::handlePost:addParent: Failed");
                return errorHandler->handleError(
                    Status::CODE_500,
                    "TopologyController::handlePost:addParent: Failed to add link between parent and child nodes.");
            }

            uint64_t bandwidth = 0;
            if (reqJson.contains("bandwidth")) {
                bandwidth = reqJson["bandwidth"].get<uint64_t>();
            }

            uint64_t latency = 0;
            if (reqJson.contains("latency")) {
                latency = reqJson["latency"].get<uint64_t>();
            }

            bool success = topology->addLinkProperty(parentId, childId, bandwidth, latency);
            if (success) {
                NES_DEBUG("TopologyController::handlePost:addParent: added link property for the link between the parent {} and "
                          "child {} nodes.",
                          parentId,
                          childId);
            } else {
                NES_ERROR("TopologyController::handlePost:addParent: Failed");
                return errorHandler->handleError(Status::CODE_500,
                                                 "TopologyController::handlePost:addParent: Failed to add link property.");
            }

            //Prepare the response
            nlohmann::json response;
            response["success"] = added;
            return createResponse(Status::CODE_200, response.dump());
        } catch (nlohmann::json::exception e) {
            return errorHandler->handleError(Status::CODE_500, e.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Server Error");
        }
    }

    ENDPOINT("DELETE", "/removeAsChild", removeParent, BODY_STRING(String, request)) {
        try {
            std::string req = request.getValue("{}");
            //check if json is valid
            if (!nlohmann::json::accept(req)) {
                return errorHandler->handleError(Status::CODE_400, "Invalid JSON");
            };
            nlohmann::json reqJson = nlohmann::json::parse(req);
            auto optional = validateRequest(reqJson);
            if (optional.has_value()) {
                return optional.value();
            }
            WorkerId parentId = reqJson["parentId"].get<WorkerId>();
            WorkerId childId = reqJson["childId"].get<WorkerId>();
            // check if childID is actually a child of parentID
            auto children = topology->getChildTopologyNodeIds(parentId);
            bool contained = false;
            for (const auto& child : children) {
                if (child == childId) {
                    contained = true;
                }
            }
            if (contained) {
                bool removed = topology->removeTopologyNodeAsChild(parentId, childId);
                if (removed) {
                    NES_DEBUG("TopologyController::handlePost:removeParent: deleted link successfully");
                } else {
                    NES_ERROR("TopologyController::handlePost:removeParent: Failed");
                    return errorHandler->handleError(Status::CODE_500, "TopologyController::handlePost:removeAsParent: Failed");
                }
                //Prepare the response
                nlohmann::json response;
                response["success"] = removed;
                return createResponse(Status::CODE_200, response.dump());
            } else {
                return errorHandler->handleError(Status::CODE_400,
                                                 fmt::format("Could not remove parent for node in topology: Node with "
                                                             "childId={} is not a child of node with parentID={}.",
                                                             childId,
                                                             parentId));
            }
        } catch (nlohmann::json::exception e) {
            return errorHandler->handleError(Status::CODE_500, e.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Server Error");
        }
    }

  private:
    std::optional<std::shared_ptr<oatpp::web::protocol::http::outgoing::Response>> validateRequest(nlohmann::json reqJson) {
        if (reqJson.empty()) {
            return errorHandler->handleError(Status::CODE_400, "empty body");
        }
        if (!reqJson.contains("parentId")) {
            return errorHandler->handleError(Status::CODE_400, " Request body missing 'parentId'");
        }
        if (!reqJson.contains("childId")) {
            return errorHandler->handleError(Status::CODE_400, " Request body missing 'childId'");
        }
        WorkerId parentId = reqJson["parentId"].get<WorkerId>();
        WorkerId childId = reqJson["childId"].get<WorkerId>();
        if (parentId == childId) {
            return errorHandler->handleError(
                Status::CODE_400,
                "Could not add/remove parent for node in topology: childId and parentId must be different.");
        }

        if (!topology->nodeWithWorkerIdExists(childId)) {
            return errorHandler->handleError(
                Status::CODE_400,
                "Could not add/remove parent for node in topology: Node with childId=" + childId.toString() + " not found.");
        }

        if (!topology->nodeWithWorkerIdExists(parentId)) {
            return errorHandler->handleError(
                Status::CODE_400,
                "Could not add/remove parent for node in topology: Node with parentId=" + parentId.toString() + " not found.");
        }
        return std::nullopt;
    }

    TopologyPtr topology;
    ErrorHandlerPtr errorHandler;
};
}// namespace REST::Controller
}// namespace NES

#include OATPP_CODEGEN_END(ApiController)
#endif// NES_COORDINATOR_INCLUDE_REST_CONTROLLER_TOPOLOGYCONTROLLER_HPP_
