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

#ifndef NES_COMMON_INCLUDE_UTIL_FILEMUTEX_HPP_
#define NES_COMMON_INCLUDE_UTIL_FILEMUTEX_HPP_

#include <string>

namespace NES::Util {

/**
 * @brief This is a mutex that uses files to perform locking.
 * It implements the Mutex named requirements
 */
class FileMutex {
  public:
    /**
     * @brief Creates a FileMutex using a file specified as filePath
     * @param filePath that path to a file
     */
    explicit FileMutex(const std::string filePath);

    /**
     * @brief closes the internal file but does not release the mutex.
     * The behavior is undefined if the mutex is owned by any thread or if any thread
     * terminates while holding any ownership of the mutex.
     */
    ~FileMutex();

    FileMutex(const FileMutex&) = delete;

    FileMutex(FileMutex&&) = delete;

    FileMutex& operator=(const FileMutex&) = delete;

    FileMutex& operator=(FileMutex&&) = delete;

    /**
     * @brief lock the mutex
     */
    void lock();

    /**
     * @brief try locking the mutex
     * @return true if locking is successful
     */
    bool try_lock();

    /**
     * @brief try unlocking the file
     */
    void unlock();

  private:
    int fd;
    std::string fileName;
};

}// namespace NES::Util

#endif// NES_COMMON_INCLUDE_UTIL_FILEMUTEX_HPP_
