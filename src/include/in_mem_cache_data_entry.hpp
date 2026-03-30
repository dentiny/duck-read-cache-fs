#pragma once

#include "duckdb/common/string.hpp"
#include "page_aligned_data_chunk.hpp"

namespace duckdb {

struct InMemCacheDataEntry {
	PageAlignedDataChunk data;
	string version_tag;
};

} // namespace duckdb
