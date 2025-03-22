#include "mock_filesystem.hpp"

#include <algorithm>
#include <cstring>

namespace duckdb {

bool operator<(const MockFileSystem::ReadOper &lhs, const MockFileSystem::ReadOper &rhs) {
	return std::tie(lhs.start_offset, lhs.bytes_to_read) < std::tie(rhs.start_offset, rhs.bytes_to_read);
}
bool operator>(const MockFileSystem::ReadOper &lhs, const MockFileSystem::ReadOper &rhs) {
	return std::tie(lhs.start_offset, lhs.bytes_to_read) > std::tie(rhs.start_offset, rhs.bytes_to_read);
}
bool operator<=(const MockFileSystem::ReadOper &lhs, const MockFileSystem::ReadOper &rhs) {
	return std::tie(lhs.start_offset, lhs.bytes_to_read) <= std::tie(rhs.start_offset, rhs.bytes_to_read);
}
bool operator>=(const MockFileSystem::ReadOper &lhs, const MockFileSystem::ReadOper &rhs) {
	return std::tie(lhs.start_offset, lhs.bytes_to_read) >= std::tie(rhs.start_offset, rhs.bytes_to_read);
}
bool operator==(const MockFileSystem::ReadOper &lhs, const MockFileSystem::ReadOper &rhs) {
	return std::tie(lhs.start_offset, lhs.bytes_to_read) == std::tie(rhs.start_offset, rhs.bytes_to_read);
}
bool operator!=(const MockFileSystem::ReadOper &lhs, const MockFileSystem::ReadOper &rhs) {
	return std::tie(lhs.start_offset, lhs.bytes_to_read) != std::tie(rhs.start_offset, rhs.bytes_to_read);
}

MockFileHandle::MockFileHandle(FileSystem &file_system, string path, FileOpenFlags flags,
                               std::function<void()> close_callback_p, std::function<void()> dtor_callback_p)
    : FileHandle(file_system, path, flags), close_callback(std::move(close_callback_p)),
      dtor_callback(std::move(dtor_callback_p)) {
	// Make sure passed-in filesystem is mock filesystem.
	[[maybe_unused]] auto &fs = file_system.Cast<MockFileSystem>();
}

unique_ptr<FileHandle> MockFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                optional_ptr<FileOpener> opener) {
	std::lock_guard<std::mutex> lck(mtx);
	++file_open_invocation;
	return make_uniq<MockFileHandle>(*this, path, flags, close_callback, dtor_callback);
}
void MockFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	std::lock_guard<std::mutex> lck(mtx);
	std::memset(buffer, 'a', nr_bytes);
	read_operations.emplace_back(ReadOper {
	    .start_offset = location,
	    .bytes_to_read = nr_bytes,
	});
}

vector<MockFileSystem::ReadOper> MockFileSystem::GetSortedReadOperations() {
	std::sort(read_operations.begin(), read_operations.end(),
	          [](const auto &lhs, const auto &rhs) { return lhs < rhs; });
	return read_operations;
}

} // namespace duckdb
