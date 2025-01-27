// Unit test for disk cache filesystem.

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "disk_cache_filesystem.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "cache_filesystem_config.hpp"

using namespace duckdb;  // NOLINT

namespace {

constexpr uint64_t TEST_FILE_SIZE = 26;
const auto TEST_FILE_CONTENT = []() {
    string content(TEST_FILE_SIZE, '\0');
    for (uint64_t idx = 0; idx < TEST_FILE_SIZE; ++idx) {
        content[idx] = 'a' + idx;
    }
    return content;
}();
const auto TEST_FILENAME = StringUtil::Format("/tmp/%s", UUID::ToString(UUID::GenerateRandomUUID()));

}  // namespace

// One chunk is involved, requested bytes include only "first and last chunk".
TEST_CASE("Test on disk cache filesystem with requested chunk the first meanwhile last chunk", "[on-disk cache test]") {
    LocalFileSystem::CreateLocal()->RemoveDirectory(ON_DISK_CACHE_DIRECTORY);
    DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal()};
    
    // First uncached read.
    {
        auto handle = disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
        const uint64_t start_offset = 1;
        const uint64_t bytes_to_read = TEST_FILE_SIZE - 2;
        string content(bytes_to_read, '\0');
        const uint64_t test_block_size = 26;
        disk_cache_fs.ReadForTesting(*handle, const_cast<void*>(static_cast<const void*>(content.data())), bytes_to_read, start_offset, test_block_size);
        REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
    }

    // Second cached read.
    {
        auto handle = disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
        const uint64_t start_offset = 1;
        const uint64_t bytes_to_read = TEST_FILE_SIZE - 2;
        string content(bytes_to_read, '\0');
        const uint64_t test_block_size = 26;
        disk_cache_fs.ReadForTesting(*handle, const_cast<void*>(static_cast<const void*>(content.data())), bytes_to_read, start_offset, test_block_size);
        REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
    }
}

// Two chunks are involved, which include both first and last chunks. 
TEST_CASE("Test on disk cache filesystem with requested chunk the first and last chunk", "[on-disk cache test]") {
    LocalFileSystem::CreateLocal()->RemoveDirectory(ON_DISK_CACHE_DIRECTORY);
    DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal()};
    
    // First uncached read.
    {
        auto handle = disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
        const uint64_t start_offset = 2;
        const uint64_t bytes_to_read = 5;
        string content(bytes_to_read, '\0');
        const uint64_t test_block_size = 5;
        disk_cache_fs.ReadForTesting(*handle, const_cast<void*>(static_cast<const void*>(content.data())), bytes_to_read, start_offset, test_block_size);
        REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
    }

    // Second cached read.
    {
        auto handle = disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
        const uint64_t start_offset = 3;
        const uint64_t bytes_to_read = 4;
        string content(bytes_to_read, '\0');
        const uint64_t test_block_size = 5;
        disk_cache_fs.ReadForTesting(*handle, const_cast<void*>(static_cast<const void*>(content.data())), bytes_to_read, start_offset, test_block_size);
        REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
    }
}

// Three blocks involved, which include first, last and middle chunk.
TEST_CASE("Test on disk cache filesystem with requested chunk the first, middle and last chunk", "[on-disk cache test]") {
    LocalFileSystem::CreateLocal()->RemoveDirectory(ON_DISK_CACHE_DIRECTORY);
    DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal()};
    
    // First uncached read.
    {
        auto handle = disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
        const uint64_t start_offset = 2;
        const uint64_t bytes_to_read = 11;
        string content(bytes_to_read, '\0');
        const uint64_t test_block_size = 5;
        disk_cache_fs.ReadForTesting(*handle, const_cast<void*>(static_cast<const void*>(content.data())), bytes_to_read, start_offset, test_block_size);
        REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
    }

    // Second cached read.
    {
        auto handle = disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
        const uint64_t start_offset = 3;
        const uint64_t bytes_to_read = 10;
        string content(bytes_to_read, '\0');
        const uint64_t test_block_size = 5;
        disk_cache_fs.ReadForTesting(*handle, const_cast<void*>(static_cast<const void*>(content.data())), bytes_to_read, start_offset, test_block_size);
        REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
    }
}

// All chunks cached locally, later access shouldn't create new cache file.
TEST_CASE("Test on disk cache filesystem no new cache file after a full cache", "[on-disk cache test]") {
    LocalFileSystem::CreateLocal()->RemoveDirectory(ON_DISK_CACHE_DIRECTORY);
    DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal()};
    
    // First uncached read.
    {
        auto handle = disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
        const uint64_t start_offset = 0;
        const uint64_t bytes_to_read = TEST_FILE_SIZE;
        string content(bytes_to_read, '\0');
        const uint64_t test_block_size = 5;
        disk_cache_fs.ReadForTesting(*handle, const_cast<void*>(static_cast<const void*>(content.data())), bytes_to_read, start_offset, test_block_size);
        REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
    }

    // Get all cache files.
    vector<string> cache_files1;
    REQUIRE(LocalFileSystem::CreateLocal()->ListFiles(ON_DISK_CACHE_DIRECTORY, [&cache_files1](const string& fname, bool /*unused*/) {
        cache_files1.emplace_back(fname);
    }));

    // Second cached read.
    {
        auto handle = disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
        const uint64_t start_offset = 3;
        const uint64_t bytes_to_read = 10;
        string content(bytes_to_read, '\0');
        const uint64_t test_block_size = 5;
        disk_cache_fs.ReadForTesting(*handle, const_cast<void*>(static_cast<const void*>(content.data())), bytes_to_read, start_offset, test_block_size);
        REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
    }

    // Get all cache files and check unchanged.
    vector<string> cache_files2;
    REQUIRE(LocalFileSystem::CreateLocal()->ListFiles(ON_DISK_CACHE_DIRECTORY, [&cache_files2](const string& fname, bool /*unused*/) {
        cache_files2.emplace_back(fname);
    }));
    REQUIRE(cache_files1 == cache_files2);
}

TEST_CASE("Test on disk cache filesystem read from end of file", "[on-disk cache test]") {
    LocalFileSystem::CreateLocal()->RemoveDirectory(ON_DISK_CACHE_DIRECTORY);
    DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal()};
    
    auto handle = disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = TEST_FILE_SIZE;
    const uint64_t bytes_to_read = TEST_FILE_SIZE;
    const uint64_t test_block_size = 5;
    const int64_t bytes_read = disk_cache_fs.ReadForTesting(*handle, /*buffer=*/nullptr, bytes_to_read, start_offset, test_block_size);
    REQUIRE(bytes_read == 0);
}

int main(int argc, char** argv) {
    auto local_filesystem = LocalFileSystem::CreateLocal();
    auto file_handle = local_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
    local_filesystem->Write(*file_handle, const_cast<void*>(static_cast<const void*>(TEST_FILE_CONTENT.data())), TEST_FILE_SIZE, /*location=*/0);
    file_handle->Sync();
    file_handle->Close();

    int result = Catch::Session().run(argc, argv);
    local_filesystem->RemoveFile(TEST_FILENAME);
    return result;
}
