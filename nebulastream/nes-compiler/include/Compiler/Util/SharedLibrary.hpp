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
#ifndef NES_COMPILER_INCLUDE_COMPILER_UTIL_SHAREDLIBRARY_HPP_
#define NES_COMPILER_INCLUDE_COMPILER_UTIL_SHAREDLIBRARY_HPP_
#include <Compiler/DynamicObject.hpp>
#include <memory>
namespace NES::Compiler {

class SharedLibrary;
using SharedLibraryPtr = std::shared_ptr<SharedLibrary>;

/**
 * @brief Represents a @DynamicObject, which relies on a shared library.
 */
class SharedLibrary : public DynamicObject {
  public:
    /**
     * @brief Creates a new @SharedLibrary Object
     * @param shareLib
     * @param soAbsolutePath: absolute path where so file is stored
     */
    explicit SharedLibrary(void* shareLib, std::string soAbsolutePath);
    /**
     * @brief Loads a shared library from a specific path.
     * @param absoluteFilePath: absolute path where so file is stored
     * @return SharedLibraryPtr
     */
    static SharedLibraryPtr load(const std::string& absoluteFilePath);

    /**
     * @brief Destructor for the shared library.
     * Automatically unloads the shared library from memory.
     */
    ~SharedLibrary() override;

  protected:
    /**
     * @brief Returns a untyped function pointer to a specific symbol.
     * @param member on the dynamic object, currently provided as a MangledName.
     * @return function ptr
     */
    [[nodiscard]] void* getInvocableFunctionPtr(const std::string& member) override;

  private:
    void* shareLib;
    std::string soAbsolutePath;
};

}// namespace NES::Compiler

#endif// NES_COMPILER_INCLUDE_COMPILER_UTIL_SHAREDLIBRARY_HPP_
