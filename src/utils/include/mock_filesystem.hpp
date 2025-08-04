// This file defines mock filesystem, which is used for testing.
// It checks a few things:
// 1. Whether bytes to read to correct (whether request is correctly chunked and cached).
// 2. Whether file handles are properly closed and destructed.

#pragma once

#include "duckdb/common/deque.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

#include <cstdint>
#include <functional>
#include <mutex>

namespace duckdb {

class MockFileHandle : public FileHandle {
public:
	MockFileHandle(FileSystem &file_system, string path, FileOpenFlags flags, std::function<void()> close_callback_p,
	               std::function<void()> dtor_callback_p);
	~MockFileHandle() override {
		D_ASSERT(dtor_callback);
		dtor_callback();
	}
	void Close() override {
		D_ASSERT(close_callback);
		close_callback();
	}

private:
	std::function<void()> close_callback;
	std::function<void()> dtor_callback;
};

class MockFileSystem : public FileSystem {
public:
	struct ReadOper {
		uint64_t start_offset = 0;
		int64_t bytes_to_read = 0;
	};

	MockFileSystem(std::function<void()> close_callback_p, std::function<void()> dtor_callback_p)
	    : close_callback(std::move(close_callback_p)), dtor_callback(std::move(dtor_callback_p)) {
	}
	~MockFileSystem() override = default;

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener) override;
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override {
		++glob_invocation;
		if (!glob_returns.empty()) {
			vector<OpenFileInfo> cur_glob_ret;
			cur_glob_ret.emplace_back(std::move(glob_returns.front()));
			glob_returns.pop_front();
			return cur_glob_ret;
		}
		return {};
	}
	int64_t GetFileSize(FileHandle &handle) override {
		++get_file_size_invocation;
		return file_size;
	}
	time_t GetLastModifiedTime(FileHandle &handle) override {
		++get_last_mod_time_invocation;
		return last_modification_time;
	}
	void Seek(FileHandle &handle, idx_t location) override {
	}
	std::string GetName() const override {
		return "mock filesystem";
	}

	// Set first N glob invocation returns, later calls will return default value.
	void SetGlobResults(vector<OpenFileInfo> file_open_infos) {
		glob_returns = deque<OpenFileInfo>{std::make_move_iterator(file_open_infos.begin()), std::make_move_iterator(file_open_infos.end())};
	}
	void SetFileSize(int64_t file_size_p) {
		file_size = file_size_p;
	}
	void SetLastModificationTime(time_t last_modification_time_p) {
		last_modification_time = last_modification_time_p;
	}
	vector<ReadOper> GetSortedReadOperations();
	uint64_t GetFileOpenInvocation() const {
		return file_open_invocation;
	}
	uint64_t GetGlobInvocation() const {
		return glob_invocation;
	}
	uint64_t GetSizeInvocation() const {
		return get_file_size_invocation;
	}
	uint64_t GetLastModTimeInvocation() const {
		return get_last_mod_time_invocation;
	}
	void ClearReadOperations() {
		read_operations.clear();
	}

private:
	int64_t file_size = 0;
	time_t last_modification_time = static_cast<time_t>(0);
	// Glob returns value for each invocation.
	std::deque<OpenFileInfo> glob_returns;
	std::function<void()> close_callback;
	std::function<void()> dtor_callback;

	uint64_t file_open_invocation = 0;     // Number of `FileOpen` gets called.
	uint64_t glob_invocation = 0;          // Number of `Glob` gets called.
	uint64_t get_file_size_invocation = 0; // Number of `GetFileSize` get called.
	uint64_t get_last_mod_time_invocation = 0; // Number of `GetLastModificationTime` called.
	vector<ReadOper> read_operations;
	std::mutex mtx;
};

bool operator<(const MockFileSystem::ReadOper &lhs, const MockFileSystem::ReadOper &rhs);
bool operator>(const MockFileSystem::ReadOper &lhs, const MockFileSystem::ReadOper &rhs);
bool operator<=(const MockFileSystem::ReadOper &lhs, const MockFileSystem::ReadOper &rhs);
bool operator>=(const MockFileSystem::ReadOper &lhs, const MockFileSystem::ReadOper &rhs);
bool operator==(const MockFileSystem::ReadOper &lhs, const MockFileSystem::ReadOper &rhs);
bool operator!=(const MockFileSystem::ReadOper &lhs, const MockFileSystem::ReadOper &rhs);

} // namespace duckdb
