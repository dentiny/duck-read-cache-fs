
#include "catch/catch.hpp"

#include "cache_filesystem.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "hffs.hpp"
#include "test_utils.hpp"

using namespace duckdb; // NOLINT

namespace {

const std::string TEST_CONTENT = "helloworld";
const std::string TEST_FILEPATH = "/tmp/testfile";

struct BaseCacheFilesystemFixture {
	BaseCacheFilesystemFixture() {
		auto local_filesystem = LocalFileSystem::CreateLocal();
		auto file_handle = local_filesystem->OpenFile(TEST_FILEPATH, FileOpenFlags::FILE_FLAGS_WRITE |
		                                                                 FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
		local_filesystem->Write(*file_handle, const_cast<char *>(TEST_CONTENT.data()), TEST_CONTENT.length(),
		                        /*location=*/0);
		file_handle->Sync();
	}
	~BaseCacheFilesystemFixture() {
		LocalFileSystem::CreateLocal()->RemoveFile(TEST_FILEPATH);
	}
};

} // namespace

// A more ideal unit test would be, we could check hugging face filesystem will
// be used for certains files.
TEST_CASE_METHOD(BaseCacheFilesystemFixture, "Test cached filesystem CanHandle", "[base cache filesystem]") {
	auto instance_state = make_shared_ptr<CacheHttpfsInstanceState>();
	InitializeCacheReaderForTest(instance_state, instance_state->config);
	unique_ptr<FileSystem> vfs = make_uniq<VirtualFileSystem>();
	vfs->RegisterSubSystem(make_uniq<CacheFileSystem>(make_uniq<HuggingFaceFileSystem>(), instance_state));
	vfs->RegisterSubSystem(make_uniq<CacheFileSystem>(make_uniq<LocalFileSystem>(), std::move(instance_state)));

	// VFS can handle local files with cached local filesystem.
	auto file_handle = vfs->OpenFile(TEST_FILEPATH, FileOpenFlags::FILE_FLAGS_READ);
	// Check casting success to make sure disk cache filesystem is selected,
	// rather than the fallback local filesystem within virtual filesystem.
	[[maybe_unused]] auto &cached_file_handle = file_handle->Cast<CacheFileSystemHandle>();
}
