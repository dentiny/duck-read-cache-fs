// Unit test for disk cache filesystem.
//
// IO operations are performed in chunks, testing senarios are listed as
// follows:
// (1) There're one or more chunks to read;
// (2) Chunks to read include start, middle and end parts of the file;
// (3) Bytes to read include partial or complete part of a chunk;
// (4) Chunks to read is not cached, partially cached, or completed cached.
// These senarios are orthogonal and should be tested in their combinations.

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "disk_cache_filesystem.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "cache_filesystem_config.hpp"

using namespace duckdb; // NOLINT

namespace {

constexpr uint64_t TEST_FILE_SIZE = 26;
const auto TEST_FILE_CONTENT = []() {
  string content(TEST_FILE_SIZE, '\0');
  for (uint64_t idx = 0; idx < TEST_FILE_SIZE; ++idx) {
    content[idx] = 'a' + idx;
  }
  return content;
}();
const auto TEST_FILENAME =
    StringUtil::Format("/tmp/%s", UUID::ToString(UUID::GenerateRandomUUID()));
const auto TEST_ON_DISK_CACHE_DIRECTORY = "/tmp/duckdb_test_cached_http_cache";
const OnDiskCacheConfig TEST_CACHE_CONFIG = []() {
  OnDiskCacheConfig cache_config;
  cache_config.on_disk_cache_directory = TEST_ON_DISK_CACHE_DIRECTORY;
  return cache_config;
}();
} // namespace

// One chunk is involved, requested bytes include only "first and last chunk".
TEST_CASE("Test on disk cache filesystem with requested chunk the first "
          "meanwhile last chunk",
          "[on-disk cache filesystem test]") {
  LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
  DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal(),
                                    TEST_CACHE_CONFIG};

  // First uncached read.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 1;
    const uint64_t bytes_to_read = TEST_FILE_SIZE - 2;
    string content(bytes_to_read, '\0');
    const uint64_t test_block_size = 26;
    const uint64_t bytes_read = disk_cache_fs.ReadForTesting(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset, test_block_size);
    REQUIRE(bytes_read == bytes_to_read);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }

  // Second cached read.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 1;
    const uint64_t bytes_to_read = TEST_FILE_SIZE - 2;
    string content(bytes_to_read, '\0');
    const uint64_t test_block_size = 26;
    const uint64_t bytes_read = disk_cache_fs.ReadForTesting(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset, test_block_size);
    REQUIRE(bytes_read == bytes_to_read);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }
}

// Two chunks are involved, which include both first and last chunks.
TEST_CASE("Test on disk cache filesystem with requested chunk the first and "
          "last chunk",
          "[on-disk cache filesystem test]") {
  LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
  DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal(),
                                    TEST_CACHE_CONFIG};

  // First uncached read.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 2;
    const uint64_t bytes_to_read = 5;
    string content(bytes_to_read, '\0');
    const uint64_t test_block_size = 5;
    const uint64_t bytes_read = disk_cache_fs.ReadForTesting(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset, test_block_size);
    REQUIRE(bytes_read == bytes_to_read);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }

  // Second cached read.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 3;
    const uint64_t bytes_to_read = 4;
    string content(bytes_to_read, '\0');
    const uint64_t test_block_size = 5;
    const uint64_t bytes_read = disk_cache_fs.ReadForTesting(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset, test_block_size);
    REQUIRE(bytes_read == bytes_to_read);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }
}

// Three blocks involved, which include first, last and middle chunk.
TEST_CASE("Test on disk cache filesystem with requested chunk the first, "
          "middle and last chunk",
          "[on-disk cache filesystem test]") {
  LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
  DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal(),
                                    TEST_CACHE_CONFIG};

  // First uncached read.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 2;
    const uint64_t bytes_to_read = 11;
    string content(bytes_to_read, '\0');
    const uint64_t test_block_size = 5;
    const uint64_t bytes_read = disk_cache_fs.ReadForTesting(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset, test_block_size);
    REQUIRE(bytes_read == bytes_to_read);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }

  // Second cached read.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 3;
    const uint64_t bytes_to_read = 10;
    string content(bytes_to_read, '\0');
    const uint64_t test_block_size = 5;
    const uint64_t bytes_read = disk_cache_fs.ReadForTesting(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset, test_block_size);
    REQUIRE(bytes_read == bytes_to_read);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }
}

// One block involved, it's the first meanwhile last block; requested content
// doesn't involve the end of the file.
TEST_CASE(
    "Test on disk cache filesystem with requested chunk first and last one",
    "[on-disk cache filesystem test]") {
  LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
  DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal(),
                                    TEST_CACHE_CONFIG};

  // First uncached read.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 2;
    const uint64_t bytes_to_read = 2;
    string content(bytes_to_read, '\0');
    const uint64_t test_block_size = 5;
    const uint64_t bytes_read = disk_cache_fs.ReadForTesting(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset, test_block_size);
    REQUIRE(bytes_read == bytes_to_read);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }

  // Second cached read.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 3;
    const uint64_t bytes_to_read = 2;
    string content(bytes_to_read, '\0');
    const uint64_t test_block_size = 5;
    const uint64_t bytes_read = disk_cache_fs.ReadForTesting(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset, test_block_size);
    REQUIRE(bytes_read == bytes_to_read);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }
}

// Requested chunk involves the end of the file.
TEST_CASE("Test on disk cache filesystem with requested chunk at last of file",
          "[on-disk cache filesystem test]") {
  LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
  DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal(),
                                    TEST_CACHE_CONFIG};

  // First uncached read.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 23;
    const uint64_t bytes_to_read = 10;
    string content(3, '\0'); // effective offset range: [23, 25]
    const uint64_t test_block_size = 5;
    const uint64_t bytes_read = disk_cache_fs.ReadForTesting(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset, test_block_size);
    REQUIRE(bytes_read == 3);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }

  // Get all cache files and check file count.
  vector<string> cache_files;
  REQUIRE(LocalFileSystem::CreateLocal()->ListFiles(
      TEST_ON_DISK_CACHE_DIRECTORY,
      [&cache_files](const string &fname, bool /*unused*/) {
        cache_files.emplace_back(fname);
      }));
  REQUIRE(cache_files.size() == 2);

  // Second cached read, partial cached and another part uncached.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 15;
    const uint64_t bytes_to_read = 15;
    string content(11, '\0'); // effective offset range: [15, 25]
    const uint64_t test_block_size = 5;
    const uint64_t bytes_read = disk_cache_fs.ReadForTesting(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset, test_block_size);
    REQUIRE(bytes_read == 11);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }

  // Get all cache files and check file count.
  cache_files.clear();
  REQUIRE(LocalFileSystem::CreateLocal()->ListFiles(
      TEST_ON_DISK_CACHE_DIRECTORY,
      [&cache_files](const string &fname, bool /*unused*/) {
        cache_files.emplace_back(fname);
      }));
  REQUIRE(cache_files.size() == 3);
}

// Requested chunk involves the middle of the file.
TEST_CASE(
    "Test on disk cache filesystem with requested chunk at middle of file",
    "[on-disk cache filesystem test]") {
  LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
  DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal(),
                                    TEST_CACHE_CONFIG};

  // First uncached read.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 16;
    const uint64_t bytes_to_read = 3;
    string content(bytes_to_read, '\0'); // effective offset range: [16, 18]
    const uint64_t test_block_size = 5;
    const uint64_t bytes_read = disk_cache_fs.ReadForTesting(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset, test_block_size);
    REQUIRE(bytes_read == bytes_to_read);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }

  // Get all cache files and check file count.
  vector<string> cache_files;
  REQUIRE(LocalFileSystem::CreateLocal()->ListFiles(
      TEST_ON_DISK_CACHE_DIRECTORY,
      [&cache_files](const string &fname, bool /*unused*/) {
        cache_files.emplace_back(fname);
      }));
  REQUIRE(cache_files.size() == 1);

  // Second cached read, partial cached and another part uncached.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 8;
    const uint64_t bytes_to_read = 14;
    string content(bytes_to_read, '\0'); // effective offset range: [8, 21]
    const uint64_t test_block_size = 5;
    const uint64_t bytes_read = disk_cache_fs.ReadForTesting(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset, test_block_size);
    REQUIRE(bytes_read == bytes_to_read);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }

  // Get all cache files and check file count.
  cache_files.clear();
  REQUIRE(LocalFileSystem::CreateLocal()->ListFiles(
      TEST_ON_DISK_CACHE_DIRECTORY,
      [&cache_files](const string &fname, bool /*unused*/) {
        cache_files.emplace_back(fname);
      }));
  REQUIRE(cache_files.size() == 4);
}

// All chunks cached locally, later access shouldn't create new cache file.
TEST_CASE("Test on disk cache filesystem no new cache file after a full cache",
          "[on-disk cache filesystem test]") {
  LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
  DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal(),
                                    TEST_CACHE_CONFIG};

  // First uncached read.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 0;
    const uint64_t bytes_to_read = TEST_FILE_SIZE;
    string content(bytes_to_read, '\0');
    const uint64_t test_block_size = 5;
    const uint64_t bytes_read = disk_cache_fs.ReadForTesting(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset, test_block_size);
    REQUIRE(bytes_read == bytes_to_read);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }

  // Get all cache files.
  vector<string> cache_files1;
  REQUIRE(LocalFileSystem::CreateLocal()->ListFiles(
      TEST_ON_DISK_CACHE_DIRECTORY,
      [&cache_files1](const string &fname, bool /*unused*/) {
        cache_files1.emplace_back(fname);
      }));

  // Second cached read.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 3;
    const uint64_t bytes_to_read = 10;
    string content(bytes_to_read, '\0');
    const uint64_t test_block_size = 5;
    const uint64_t bytes_read = disk_cache_fs.ReadForTesting(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset, test_block_size);
    REQUIRE(bytes_read == bytes_to_read);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }

  // Get all cache files and check unchanged.
  vector<string> cache_files2;
  REQUIRE(LocalFileSystem::CreateLocal()->ListFiles(
      TEST_ON_DISK_CACHE_DIRECTORY,
      [&cache_files2](const string &fname, bool /*unused*/) {
        cache_files2.emplace_back(fname);
      }));
  REQUIRE(cache_files1 == cache_files2);
}

TEST_CASE("Test on disk cache filesystem read from end of file",
          "[on-disk cache filesystem test]") {
  LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
  DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal(),
                                    TEST_CACHE_CONFIG};

  auto handle =
      disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
  const uint64_t start_offset = TEST_FILE_SIZE;
  const uint64_t bytes_to_read = TEST_FILE_SIZE;
  const uint64_t test_block_size = 5;
  const int64_t bytes_read =
      disk_cache_fs.ReadForTesting(*handle, /*buffer=*/nullptr, bytes_to_read,
                                   start_offset, test_block_size);
  REQUIRE(bytes_read == 0);
}

TEST_CASE("Test on reading non-existent file",
          "[on-disk cache filesystem test]") {
  LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
  DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal(),
                                    TEST_CACHE_CONFIG};
  REQUIRE_THROWS(disk_cache_fs.OpenFile("non-existent-file",
                                        FileOpenFlags::FILE_FLAGS_READ));
}

int main(int argc, char **argv) {
  auto local_filesystem = LocalFileSystem::CreateLocal();
  auto file_handle = local_filesystem->OpenFile(
      TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE |
                         FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
  local_filesystem->Write(
      *file_handle,
      const_cast<void *>(static_cast<const void *>(TEST_FILE_CONTENT.data())),
      TEST_FILE_SIZE, /*location=*/0);
  file_handle->Sync();
  file_handle->Close();

  int result = Catch::Session().run(argc, argv);
  local_filesystem->RemoveFile(TEST_FILENAME);
  return result;
}
