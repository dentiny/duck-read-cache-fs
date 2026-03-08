#include "page_aligned_data_chunk.hpp"

#include "duckdb/common/assert.hpp"
#include "filesystem_utils.hpp"

#include <cstdlib>
#include <cstring>
#include <new>

#if defined(_WIN32)
#include <malloc.h>
#endif

namespace duckdb {

void PageAlignedDeleter::operator()(char *p) const noexcept {
	if (p == nullptr) {
		return;
	}
#if defined(_WIN32)
	_aligned_free(p);
#else
	std::free(p);
#endif
}

void PageAlignedDataChunk::CopyFrom(const void *src, idx_t src_length) {
	if (src_length > capacity) {
		throw InvalidInputException("CopyFrom: src_length is greater than capacity");
	}
	std::memcpy(chunk.get(), src, src_length);
	length = src_length;
}

void PageAlignedDataChunk::CopyTo(char *dest, idx_t src_offset, idx_t copy_length) const {
	if (src_offset + copy_length > length) {
		throw InvalidInputException("src_offset + copy_length is greater than length");
	}
	std::memmove(dest, chunk.get() + src_offset, copy_length);
}

PageAlignedDataChunk AllocatePageAlignedChunk(idx_t requested_length) {
	if (requested_length == 0) {
		return PageAlignedDataChunk {};
	}
	const idx_t alignment = GetFileSystemPageSize();
	// Capacity rounded up to page size for direct IO; length starts at 0.
	const idx_t capacity = ((requested_length + alignment - 1) / alignment) * alignment;

#if defined(_WIN32)
	void *ptr = _aligned_malloc(static_cast<size_t>(capacity), static_cast<size_t>(alignment));
	if (ptr == nullptr) {
		throw std::bad_alloc();
	}
	return PageAlignedDataChunk {
	    .chunk = unique_ptr<char[], PageAlignedDeleter>(static_cast<char *>(ptr)),
	    .length = 0,
	    .capacity = capacity,
	};
#else
	void *ptr = nullptr;
	if (posix_memalign(&ptr, static_cast<size_t>(alignment), static_cast<size_t>(capacity)) != 0) {
		throw std::bad_alloc();
	}
	return PageAlignedDataChunk {
	    .chunk = unique_ptr<char[], PageAlignedDeleter>(static_cast<char *>(ptr)),
	    .length = 0,
	    .capacity = capacity,
	};
#endif
}

} // namespace duckdb
