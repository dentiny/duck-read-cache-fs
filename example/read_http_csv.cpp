#include "duckdb.hpp"

using namespace duckdb;

int main() {
	DuckDB db(nullptr);

	Connection con(db);

	auto res = con.Query("LOAD read_cache_fs;");
	res->Print();

	res = con.Query("select * from read_csv_auto('https://csvbase.com/meripaterson/stock-exchanges');");
	res->Print();
	
	return 0;
}
