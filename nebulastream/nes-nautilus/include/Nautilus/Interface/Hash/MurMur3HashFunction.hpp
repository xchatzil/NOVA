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
#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_HASH_MURMUR3HASHFUNCTION_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_HASH_MURMUR3HASHFUNCTION_HPP_
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>

namespace NES::Nautilus::Interface {

/**
 * @brief Implementation of the MurMur3 hash function for nautilus types.
 * This implementations is based on the hash functions of https://github.com/martinus/robin-hood-hashing/ and duckdb.
 */
class MurMur3HashFunction : public HashFunction {
  public:
    // Seed as an initialisation.
    const uint64_t SEED = 902850234;

    /**
     * @brief Inits the HashValue with the seed
     * @return HashValue
     */
    HashValue init() override;

    /**
     * @brief Calculates the hash of value and xor-es it with hash
     * @param hash
     * @param value
     * @return HashValue
     */
    HashValue calculate(HashValue& hash, Value<>& value) override;

    /**
     * @brief Calculates the hash of value and xor-es it with hash (passes the value over to calculate(hash, value) after hash init())
     * @param value
     * @return HashValue
     */
    HashValue calculate(Value<>& value);

    /**
     * @brief Do not use this method for MurMur3Hash, we require this only until issue #3648 has been fixed TODO
     * @param hash
     * @param value
     * @param state
     * @return HashValue
     */
    HashValue calculateWithState(HashValue& hash, Value<>& value, Value<MemRef>& state) override;
};
}// namespace NES::Nautilus::Interface

#endif// NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_HASH_MURMUR3HASHFUNCTION_HPP_
