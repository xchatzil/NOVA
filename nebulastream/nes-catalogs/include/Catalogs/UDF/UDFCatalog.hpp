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

#ifndef NES_CATALOGS_INCLUDE_CATALOGS_UDF_UDFCATALOG_HPP_
#define NES_CATALOGS_INCLUDE_CATALOGS_UDF_UDFCATALOG_HPP_

#include <string>
#include <unordered_map>
#include <vector>

#include <Operators/LogicalOperators/UDFs/JavaUDFDescriptor.hpp>
#include <Operators/LogicalOperators/UDFs/PythonUDFDescriptor.hpp>
#include <Operators/LogicalOperators/UDFs/UDFDescriptor.hpp>

namespace NES::Catalogs::UDF {

/**
 * @brief The UDF catalog stores all the data required to execute a Java UDF inside an embedded JVM.
 *
 * It provides an API to register and remove UDFs from a client,
 * to retrieve the implementation data during query rewrite,
 * and to retrieve a list of registered UDFs for visualization.
 */
class UDFCatalog {
  public:
    /**
     * @brief Create a UDFCatalog instance.
     * @return UDFCatalog instance.
     */
    static std::unique_ptr<UDFCatalog> create();

    /**
     * @brief Register the descriptor data of a UDF. UDFs can not share the same name,
     * even if they are implemented in different programming languages.
     * @param name The name of the UDF as it is used in queryIdAndCatalogEntryMapping.
     * @param descriptor The implementation data of the UDF.
     * @throws UdfException If descriptor is a nullptr or if a UDF under the name is already registered.
     */
    void registerUDF(const std::string& name, UDFDescriptorPtr descriptor);

    /**
     * @brief Retrieve the implementation data for a UDF.
     * @param name The name of the UDF as it is used in queryIdAndCatalogEntryMapping.
     * @return The implementation data of the UDF, or nullptr if the UDF is not registered.
     */
    UDFDescriptorPtr getUDFDescriptor(const std::string& name);

    /**
     * @brief Remove the UDF from the catalog.
     * @param name The name of the UDF as it is used in queryIdAndCatalogEntryMapping.
     * @return True, if the UDF was registered in the catalog; otherwise, False.
     *
     * Removing an unregistered UDF is not an error condition because it could have been removed by another user.
     * In this case, the user should just be notified.
     */
    bool removeUDF(const std::string& name);

    /**
     * @brief Retrieve a list of registered UDFs.
     * @return A STL container containing the names of the registered UDFs.
     */
    // TODO Not sure if this is the correct API.
    // I'd rather return an iterator over the keys. Or maybe we don't need this function at all.
    // It's not really used to support UDFs. It could be used by a visualization tool.
    // It would be nice to show more information about the UDFs (signature, size of dependencies),
    // but that's not needed right now.
    std::vector<std::string> listUDFs() const;

  private:
    std::unordered_map<std::string, UDFDescriptorPtr> udfStore;
};

}// namespace NES::Catalogs::UDF
#endif// NES_CATALOGS_INCLUDE_CATALOGS_UDF_UDFCATALOG_HPP_
