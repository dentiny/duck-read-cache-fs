#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

#include "duckdb.hpp"

// Mutex for synchronized console output
std::mutex cout_mutex;

// Atomic counter for completed iterations
std::atomic<int> completed_iterations {0};
std::atomic<int> failed_iterations {0};

void log(int thread_id, const std::string &msg) {
	std::lock_guard<std::mutex> lock(cout_mutex);
	std::cout << "[Thread " << thread_id << "] " << msg << '\n';
}

void log_error(int thread_id, const std::string &msg) {
	std::lock_guard<std::mutex> lock(cout_mutex);
	std::cerr << "[Thread " << thread_id << " ERROR] " << msg << '\n';
}

// Execute a SQL statement and optionally print results
void execute(duckdb::Connection &conn, const std::string &sql, int thread_id, bool print_result = false) {
	auto result = conn.Query(sql);
	if (result->HasError()) {
		log_error(thread_id, "SQL Error: " + result->GetError() + "\nSQL: " + sql);
		throw std::runtime_error(result->GetError());
	}
	if (print_result && result->RowCount() > 0) {
		log(thread_id, "Result: " + result->ToString());
	}
}

// Setup function: create shared parquet files in S3
void setup_shared_data(const std::string &extension_path, const std::string &data_path) {
	std::cout << "[Setup] Creating shared test data in " << data_path << '\n';

	duckdb::DBConfig config;
	config.options.allow_unsigned_extensions = true;
	duckdb::DuckDB db(nullptr, &config);
	duckdb::Connection conn(db);

	// Load extensions
	conn.Query("LOAD '" + extension_path + "';");
	conn.Query("INSTALL tpch;");
	conn.Query("LOAD tpch;");

	// Configure S3/MinIO
	conn.Query(R"(
        CREATE SECRET minio_secret (
            TYPE S3,
            KEY_ID 'minioadmin',
            SECRET 'minioadmin',
            REGION 'us-east-1',
            ENDPOINT 'localhost:19000',
            USE_SSL false,
            URL_STYLE 'path'
        );
    )");

	// Configure cache
	conn.Query("SET cache_httpfs_type = 'on_disk';");

	// Generate TPCH data
	std::cout << "[Setup] Generating TPCH data (sf=0.1)..." << '\n';
	conn.Query("CALL dbgen(sf=0.1);");

	// Write directly as Parquet files to S3
	std::cout << "[Setup] Writing Parquet files to S3..." << '\n';
	conn.Query("COPY lineitem TO '" + data_path + "/lineitem.parquet' (FORMAT PARQUET);");
	conn.Query("COPY orders TO '" + data_path + "/orders.parquet' (FORMAT PARQUET);");
	conn.Query("COPY customer TO '" + data_path + "/customer.parquet' (FORMAT PARQUET);");

	// Clear cache so threads start fresh
	conn.Query("SELECT cache_httpfs_clear_cache();");

	std::cout << "[Setup] Data setup complete!" << '\n';
}

// Helper to create and configure a DuckDB instance
struct DuckDBClient {
	std::unique_ptr<duckdb::DuckDB> db;
	std::unique_ptr<duckdb::Connection> conn;
	int client_id;
	int thread_id;

	DuckDBClient(int thread_id_, int client_id_, const std::string &extension_path)
	    : client_id(client_id_), thread_id(thread_id_) {
		log(thread_id, "Creating client " + std::to_string(client_id));

		duckdb::DBConfig config;
		config.options.allow_unsigned_extensions = true;
		db = std::make_unique<duckdb::DuckDB>(nullptr, &config);
		conn = std::make_unique<duckdb::Connection>(*db);

		// Load extension
		execute(*conn, "LOAD '" + extension_path + "';", thread_id);

		// Configure S3/MinIO credentials
		execute(*conn, R"(
            CREATE SECRET minio_secret (
                TYPE S3,
                KEY_ID 'minioadmin',
                SECRET 'minioadmin',
                REGION 'us-east-1',
                ENDPOINT 'localhost:19000',
                USE_SSL false,
                URL_STYLE 'path'
            );
        )",
		        thread_id);

		// Configure cache_httpfs settings
		execute(*conn, "SET cache_httpfs_type = 'on_disk';", thread_id);
		execute(*conn, "SET cache_httpfs_profile_type = 'temp';", thread_id);
		execute(*conn, "SET cache_httpfs_cache_block_size = 262144;", thread_id); // 256KB blocks

		// Clear profile for this client's stats
		execute(*conn, "SELECT cache_httpfs_clear_profile();", thread_id);

		log(thread_id, "Client " + std::to_string(client_id) + " ready");
	}

	~DuckDBClient() {
		log(thread_id, "Destroying client " + std::to_string(client_id));
		// Connection and DB will be destroyed automatically
	}

	void run_query(const std::string &sql) {
		execute(*conn, sql, thread_id);
	}

	void print_profile() {
		auto result = conn->Query("SELECT cache_httpfs_get_profile();");
		if (!result->HasError() && result->RowCount() > 0) {
			log(thread_id, "Client " + std::to_string(client_id) + " Profile:\n" + result->GetValue(0, 0).ToString());
		}
	}
};

// Run a set of queries on a client
void run_queries(DuckDBClient &client, const std::string &data_path, const std::string &phase) {
	log(client.thread_id, "Client " + std::to_string(client.client_id) + " running queries (" + phase + ")");

	// Query 1 - aggregation on lineitem
	client.run_query("SELECT COUNT(*), SUM(l_quantity), AVG(l_extendedprice) "
	                 "FROM read_parquet('" +
	                 data_path +
	                 "/lineitem.parquet') "
	                 "WHERE l_shipdate >= '1995-01-01';");

	// Query 2 - different columns
	client.run_query("SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber "
	                 "FROM read_parquet('" +
	                 data_path +
	                 "/lineitem.parquet') "
	                 "WHERE l_discount > 0.05 LIMIT 500;");

	// Query 3 - join across files
	client.run_query("SELECT c.c_name, COUNT(o.o_orderkey) as order_count, SUM(o.o_totalprice) as total_spent "
	                 "FROM read_parquet('" +
	                 data_path +
	                 "/customer.parquet') c "
	                 "JOIN read_parquet('" +
	                 data_path +
	                 "/orders.parquet') o ON c.c_custkey = o.o_custkey "
	                 "GROUP BY c.c_name ORDER BY total_spent DESC LIMIT 20;");
}

// Concurrent read test with staggered client lifecycle
// Pattern: start client1 -> read -> start client2 -> read both -> destroy client2 -> read client1 -> destroy client1
void run_read_test(int thread_id, int iteration, const std::string &extension_path, const std::string &data_path) {
	log(thread_id, "Starting iteration " + std::to_string(iteration) + " with staggered client lifecycle");

	try {
		// Phase 1: Start client 1 only
		log(thread_id, "=== Phase 1: Client 1 only ===");
		auto client1 = std::make_unique<DuckDBClient>(thread_id, 1, extension_path);
		std::this_thread::sleep_for(std::chrono::milliseconds(50));

		// Phase 2: Read with client 1
		log(thread_id, "=== Phase 2: Reading with Client 1 ===");
		run_queries(*client1, data_path, "client1 solo");
		std::this_thread::sleep_for(std::chrono::milliseconds(50));

		// Phase 3: Start client 2 (while client 1 still exists)
		log(thread_id, "=== Phase 3: Starting Client 2 ===");
		auto client2 = std::make_unique<DuckDBClient>(thread_id, 2, extension_path);
		std::this_thread::sleep_for(std::chrono::milliseconds(50));

		// Phase 4: Read with both clients concurrently
		log(thread_id, "=== Phase 4: Reading with both clients ===");
		run_queries(*client1, data_path, "client1 with client2 alive");
		run_queries(*client2, data_path, "client2 with client1 alive");
		std::this_thread::sleep_for(std::chrono::milliseconds(50));

		// Phase 5: Destroy client 2 (while client 1 still active)
		log(thread_id, "=== Phase 5: Destroying Client 2 ===");
		client2->print_profile();
		client2.reset(); // Explicitly destroy client 2
		std::this_thread::sleep_for(std::chrono::milliseconds(50));

		// Phase 6: Read with client 1 (after client 2 destroyed)
		log(thread_id, "=== Phase 6: Reading with Client 1 (after Client 2 destroyed) ===");
		run_queries(*client1, data_path, "client1 after client2 destroyed");
		std::this_thread::sleep_for(std::chrono::milliseconds(50));

		// Phase 7: Print final profile and destroy client 1
		log(thread_id, "=== Phase 7: Destroying Client 1 ===");
		client1->print_profile();
		client1.reset(); // Explicitly destroy client 1

		completed_iterations++;
		log(thread_id, "Iteration " + std::to_string(iteration) + " completed successfully!");

	} catch (const std::exception &e) {
		failed_iterations++;
		log_error(thread_id, "Exception in iteration " + std::to_string(iteration) + ": " + e.what());
	}
}

void worker_thread(int thread_id, int num_iterations, const std::string &extension_path, const std::string &data_path) {
	log(thread_id, "Worker thread started, will run " + std::to_string(num_iterations) + " iterations");

	for (int i = 0; i < num_iterations; i++) {
		run_read_test(thread_id, i, extension_path, data_path);

		// Small delay between iterations
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}

	log(thread_id, "Worker thread finished");
}

// Get absolute path relative to executable
std::string get_extension_path() {
	char cwd[1024];
	if (getcwd(cwd, sizeof(cwd)) != nullptr) {
		std::string path(cwd);
		if (path.find("stress_test/build") != std::string::npos) {
			return path.substr(0, path.find("stress_test")) +
			       "build/debug/extension/cache_httpfs/cache_httpfs.duckdb_extension";
		}
	}
	return "./cache_httpfs.duckdb_extension";
}

int main(int argc, char *argv[]) {
	int num_threads = 2;
	int iterations_per_thread = 3;
	std::string extension_path = get_extension_path();
	std::string data_path = "s3://ducklake/stress_test_parquet";

	// Parse command line arguments
	if (argc > 1) {
		num_threads = std::stoi(argv[1]);
	}
	if (argc > 2) {
		iterations_per_thread = std::stoi(argv[2]);
	}
	if (argc > 3) {
		extension_path = argv[3];
	}
	if (argc > 4) {
		data_path = argv[4];
	}

	std::cout << "=== Cache HTTPFS Stress Test ===" << '\n';
	std::cout << "Threads: " << num_threads << '\n';
	std::cout << "Iterations per thread: " << iterations_per_thread << '\n';
	std::cout << "Extension path: " << extension_path << '\n';
	std::cout << "Data path: " << data_path << '\n';
	std::cout << '\n';
	std::cout << "Make sure MinIO is running: docker-compose up -d" << '\n';
	std::cout << "================================" << '\n';
	std::cout << '\n';

	// Setup phase: create shared data once
	try {
		setup_shared_data(extension_path, data_path);
	} catch (const std::exception &e) {
		std::cerr << "[Setup ERROR] " << e.what() << '\n';
		return 1;
	}

	auto start_time = std::chrono::high_resolution_clock::now();

	// Spawn worker threads that all read from the same data
	std::vector<std::thread> threads;
	threads.reserve(num_threads);
	for (int i = 0; i < num_threads; i++) {
		threads.emplace_back(worker_thread, i, iterations_per_thread, extension_path, data_path);
	}

	// Wait for all threads to complete
	for (auto &t : threads) {
		t.join();
	}

	auto end_time = std::chrono::high_resolution_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time);

	std::cout << '\n';
	std::cout << "=== Test Results ===" << '\n';
	std::cout << "Completed iterations: " << completed_iterations.load() << '\n';
	std::cout << "Failed iterations: " << failed_iterations.load() << '\n';
	std::cout << "Total time: " << duration.count() << " seconds" << '\n';

	if (failed_iterations.load() > 0) {
		std::cout << "TEST FAILED - Some iterations had errors" << '\n';
		return 1;
	}

	std::cout << "TEST PASSED - All iterations completed successfully" << '\n';
	return 0;
}
