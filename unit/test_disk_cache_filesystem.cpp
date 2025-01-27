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

// Testing senario: (1) uncached read; (2) read block size aligned with file size. 
TEST_CASE("Test on disk cache filesystem with cache size aligned and uncached read", "[on-disk cache test]") {
    DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal()};
    auto handle = disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    string content(TEST_FILE_SIZE, '\0');
    const uint64_t test_block_size = TEST_FILE_SIZE;
    disk_cache_fs.ReadForTesting(*handle, const_cast<void*>(static_cast<const void*>(content.data())), TEST_FILE_SIZE, /*location=*/0, test_block_size);
    REQUIRE(content == TEST_FILE_CONTENT);
}

// Testing senario: (1) uncached read; (2) read block size is not aligned with file size.
TEST_CASE("Test on disk cache filesystem with cache size un-aligned and uncached read", "[on-disk cache test]") {
    DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal()};
    auto handle = disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t test_block_size = TEST_FILE_SIZE;
    const uint64_t test_location = 5;
    string content(TEST_FILE_SIZE - test_location, '\0');
    disk_cache_fs.ReadForTesting(*handle, const_cast<void*>(static_cast<const void*>(content.data())), TEST_FILE_SIZE, test_location, test_block_size);
    REQUIRE(content == TEST_FILE_CONTENT.substr(test_location));
}

// Testing senario: (1) cached read; (2) read block size is not aligned with file size.
TEST_CASE("Test on disk cache filesystem with cache size un-aligned and cached read", "[on-disk cache test]") {
    DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal()};
    const uint64_t test_block_size = TEST_FILE_SIZE;
    
    // First read caches whole test file content.
    {
        auto handle = disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);        
        string content(TEST_FILE_SIZE, '\0');
        disk_cache_fs.ReadForTesting(*handle, const_cast<void*>(static_cast<const void*>(content.data())), TEST_FILE_SIZE, /*location=*/0, test_block_size);
        REQUIRE(content == TEST_FILE_CONTENT);
    }

    // Second read hits cache file.
    {
        auto handle = disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);  
        const uint64_t test_block_size = TEST_FILE_SIZE;
        const uint64_t test_location = 5;
        string content(TEST_FILE_SIZE - test_location, '\0');
        disk_cache_fs.ReadForTesting(*handle, const_cast<void*>(static_cast<const void*>(content.data())), TEST_FILE_SIZE, test_location, test_block_size);
        REQUIRE(content == TEST_FILE_CONTENT.substr(test_location));
    }
}

int main(int argc, char** argv) {
    auto local_filesystem = LocalFileSystem::CreateLocal();
    auto file_handle = local_filesystem->OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
    local_filesystem->Write(*file_handle, const_cast<void*>(static_cast<const void*>(TEST_FILE_CONTENT.data())), TEST_FILE_SIZE, /*location=*/0);
    file_handle->Sync();
    file_handle->Close();

    int result = Catch::Session().run(argc, argv);

    // TODO(hjiang): Use `SCOPE_GUARD` to delete automatically.
    local_filesystem->RemoveFile(TEST_FILENAME);

    return result;
}
