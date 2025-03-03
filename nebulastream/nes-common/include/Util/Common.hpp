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

#ifndef NES_COMMON_INCLUDE_UTIL_COMMON_HPP_
#define NES_COMMON_INCLUDE_UTIL_COMMON_HPP_
#include <Identifiers/Identifiers.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Util/Logger/Logger.hpp>
#include <charconv>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace NES {
static constexpr auto H3_SEED = 42;
static constexpr auto NUMBER_OF_BITS_IN_HASH_VALUE = 64;
}// namespace NES

namespace NES::QueryCompilation {
enum class StreamJoinStrategy : uint8_t {
    HASH_JOIN_LOCAL,
    HASH_JOIN_VAR_SIZED,
    HASH_JOIN_GLOBAL_LOCKING,
    HASH_JOIN_GLOBAL_LOCK_FREE,
    NESTED_LOOP_JOIN
};

enum class JoinBuildSideType : uint8_t { Right, Left };
template<typename E = JoinBuildSideType, typename Out = uint64_t>
constexpr Out to_underlying(E e) noexcept {
    return static_cast<Out>(e);
}

}// namespace NES::QueryCompilation

namespace NES::Runtime::Execution {
/**
 * @brief Stores the meta date for a RecordBuffer
 */
struct BufferMetaData {
  public:
    BufferMetaData(const uint64_t watermarkTs, const SequenceData seqNumber, const OriginId originId)
        : watermarkTs(watermarkTs), seqNumber(seqNumber), originId(originId) {}

    std::string toString() const {
        std::ostringstream oss;
        oss << "waterMarkTs: " << watermarkTs << ","
            << "seqNumber: " << seqNumber << ","
            << "originId: " << originId;
        return oss.str();
    }

    const uint64_t watermarkTs;
    const SequenceData seqNumber;
    const OriginId originId;
};
}// namespace NES::Runtime::Execution

namespace NES::Util {
/**
* @brief escapes all non text characters in a input string, such that the string could be processed as json.
* @param s input string.
* @return result sing.
*/
std::string escapeJson(const std::string& str);

/**
* @brief removes leading and trailing whitespaces
*/
std::string_view trimWhiteSpaces(std::string_view in);

/**
* @brief removes leading and trailing occurences of `trimFor`
*/
std::string_view trimChar(std::string_view in, char trimFor);

namespace detail {

/**
 * @brief set of helper functions for splitting for different types
 * @return splitting function for a given type
 */
template<typename T>
struct SplitFunctionHelper {
    // Most conversions can be delegated to `std::from_chars`
    static constexpr auto FUNCTION = [](std::string_view str) {
        T result_value;
        auto trimmed = trimWhiteSpaces(str);
        auto result = std::from_chars(trimmed.data(), trimmed.data() + trimmed.size(), result_value);
        if (result.ec == std::errc::invalid_argument) {
            NES_THROW_RUNTIME_ERROR("Could not convert");
        }
        return result_value;
    };
};

/**
 * Specialization for `std::string`, which is just a copy from the string_view
 */
template<>
struct SplitFunctionHelper<std::string> {
    static constexpr auto FUNCTION = [](std::string_view x) {
        return std::string(x);
    };
};

}// namespace detail

/**
* @brief Checks if a string ends with a given string.
* @param fullString
* @param ending
* @return true if it ends with the given string, else false
*/
bool endsWith(const std::string& fullString, const std::string& ending);

/**
* @brief Checks if a string starts with a given string.
* @param fullString
* @param start
* @return true if it ends with the given string, else false
*/
uint64_t numberOfUniqueValues(std::vector<uint64_t>& values);

/**
* @brief Get number of unique elements
* @param fullString
* @param start
* @return true if it ends with the given string, else false
*/
bool startsWith(const std::string& fullString, const std::string& ending);

/**
* @brief transforms the string to an upper case version
* @param string
* @return string
*/
std::string toUpperCase(std::string string);

/**
* @brief splits a string given a delimiter into multiple substrings stored in a T vector
* the delimiter is allowed to be a string rather than a char only.
* @param data - the string that is to be split
* @param delimiter - the string that is to be split upon e.g. / or -
* @param fromStringtoT - the function that converts a string to an arbitrary type T
* @return
*/
template<typename T>
std::vector<T>
splitWithStringDelimiter(std::string_view inputString,
                         std::string_view delim,
                         std::function<T(std::string_view)> fromStringToT = detail::SplitFunctionHelper<T>::FUNCTION) {
    size_t prev_pos = 0;
    size_t next_pos = 0;
    std::vector<T> elems;

    while ((next_pos = inputString.find(delim, prev_pos)) != std::string::npos) {
        elems.push_back(fromStringToT(inputString.substr(prev_pos, next_pos - prev_pos)));
        prev_pos = next_pos + delim.size();
    }

    if (auto rest = inputString.substr(prev_pos, inputString.size()); !rest.empty()) {
        elems.push_back(fromStringToT(rest));
    }

    return elems;
}

/**
* @brief this method checks if the object is null
* @return pointer to the object
*/
template<typename T>
std::shared_ptr<T> checkNonNull(std::shared_ptr<T> ptr, const std::string& errorMessage) {
    NES_ASSERT(ptr, errorMessage);
    return ptr;
}

/**
* @brief function to replace all string occurrences
* @param data input string will be replaced in-place
* @param toSearch search string
* @param replaceStr replace string
*/
void findAndReplaceAll(std::string& data, const std::string& toSearch, const std::string& replaceStr);
/**
* @brief This function replaces the first occurrence of search term in a string with the replace term.
* @param origin - The original string that is to be manipulated
* @param search - The substring/term which we want to have replaced
* @param replace - The string that is replacing the search term.
* @return
*/
std::string replaceFirst(std::string origin, const std::string& search, const std::string& replace);

/**
 * @brief: Update the source names by sorting and then concatenating the source names from the sub- and query plan
 * @param string consumed sources of the current queryPlan
 * @param string consumed sources of the subQueryPlan
 * @return string with new source name
 */
std::string updateSourceName(std::string queryPlanSourceConsumed, std::string subQueryPlanSourceConsumed);

/**
* @brief Truncates the file and then writes the header string as is to the file
* @param csvFileName
* @param header
*/
void writeHeaderToCsvFile(const std::string& csvFileName, const std::string& header);

/**
* @brief Appends the row as is to the csv file
* @param csvFileName
* @param row
*/
void writeRowToCsvFile(const std::string& csvFileName, const std::string& row);

/**
* Partition a vector in n chunks, e.g., ([1, 2, 3, 4, 5], 3) -> [[1, 2], [3, 4], [5]]
* @param input the vector
* @param n the chunks
* @return the chunked vector
*/
template<typename T>
std::vector<std::vector<T>> partition(const std::vector<T>& vec, size_t n) {
    std::vector<std::vector<T>> outVec;
    size_t length = vec.size() / n;
    size_t remain = vec.size() % n;

    size_t begin = 0;
    size_t end = 0;
    for (size_t i = 0; i < std::min(n, vec.size()); ++i) {
        end += (remain > 0) ? (length + !!(remain--)) : length;
        outVec.push_back(std::vector<T>(vec.begin() + begin, vec.begin() + end));
        begin = end;
    }
    return outVec;
}

/**
* @brief appends newValue until the vector contains a minimum of newSize elements
* @tparam T
* @param vector the vector
* @param newSize the size of the padded vector
* @param newValue the value that should be added
*/
template<typename T>
void padVectorToSize(std::vector<T>& vector, size_t newSize, T newValue) {
    while (vector.size() < newSize) {
        vector.push_back(newValue);
    }
}

/**
* @brief hashes the key with murmur hash
 * @param key
 * @return calculated hash
 */
uint64_t murmurHash(uint64_t key);

/**
 * @brief Counts the number of lines of a string
 * @param str
 * @return number of lines
 */
uint64_t countLines(const std::string& str);

/**
 * @brief Counts the number of lines of a stream, e.g., a file
 * @param stream
 * @return number of lines
 */
uint64_t countLines(std::istream& stream);

/**
 * @brief Tries to update curVal until it succeeds or curVal is larger then newVal
 * @tparam T
 * @param curVal
 * @param newVal
 */
template<typename T>
void updateAtomicMax(std::atomic<T>& curVal, const T& newVal) {
    T prev_value = curVal;
    while (prev_value < newVal && !curVal.compare_exchange_weak(prev_value, newVal)) {
    }
};

}// namespace NES::Util

#endif// NES_COMMON_INCLUDE_UTIL_COMMON_HPP_
