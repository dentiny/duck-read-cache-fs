// This file defines mock filesystem, which is used for testing.
// It checks a few things:
// 1. Whether bytes to read to correct (whether request is correctly chunked and cached).

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/vector.hpp"

#include <cstdint>

namespace duckdb {

class MockFileHandle : public FileHandle {
public:
	MockFileHandle(FileSystem &file_system, string path, FileOpenFlags flags);
	~MockFileHandle() override = default;
	void Close() override {
		close_count++;
	}
	int64_t GetCloseCount() const {
		return close_count;
	}

private:
	// Number of `Close` invocations.
	int64_t close_count = 0;
};

class MockFileSystem : public FileSystem {
public:
	struct ReadOper {
		uint64_t start_offset = 0;
		int64_t bytes_to_read = 0;
	};

	MockFileSystem() = default;
	~MockFileSystem() override = default;

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener) override;
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t GetFileSize(FileHandle &handle) override {
		return file_size;
	}
	void Seek(FileHandle &handle, idx_t location) override {
	}
	std::string GetName() const override {
		return "mock filesystem";
	}

	void SetFileSize(int64_t file_size_p) {
		file_size = file_size_p;
	}
	vector<ReadOper> GetSortedReadOperations();
	void ClearReadOperations() {
		read_operations.clear();
	}

private:
	int64_t file_size = 0;
	vector<ReadOper> read_operations;
};

bool operator<(const MockFileSystem::ReadOper &lhs, const MockFileSystem::ReadOper &rhs);
bool operator>(const MockFileSystem::ReadOper &lhs, const MockFileSystem::ReadOper &rhs);
bool operator<=(const MockFileSystem::ReadOper &lhs, const MockFileSystem::ReadOper &rhs);
bool operator>=(const MockFileSystem::ReadOper &lhs, const MockFileSystem::ReadOper &rhs);
bool operator==(const MockFileSystem::ReadOper &lhs, const MockFileSystem::ReadOper &rhs);
bool operator!=(const MockFileSystem::ReadOper &lhs, const MockFileSystem::ReadOper &rhs);

} // namespace duckdb
