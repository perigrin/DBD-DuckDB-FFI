#include <duckdb.h>
#include <stdio.h>

int main() {
    // Create a new database
    duckdb_database db;
    duckdb_open(NULL, &db);

    // Create a new connection to the database
    duckdb_connection con;
    duckdb_connect(db, &con);

    // Create a new table
    const char *create_table_query = "CREATE TABLE IF NOT EXISTS my_table (id INTEGER, name VARCHAR)";
    duckdb_query(con, create_table_query, NULL);

    // Insert some data into the table
    const char *insert_query = "INSERT INTO my_table VALUES (1, 'John'), (2, 'Jane')";
    duckdb_query(con, insert_query, NULL);

    // Execute a query to retrieve the data
    const char *query = "SELECT * FROM my_table";
    duckdb_result result;
    duckdb_query(con, query, &result);

    // Print the results
    printf("Results:\n");
    for (size_t row = 0; row < duckdb_row_count(&result); row++) {
        int id = duckdb_value_int32(&result, 0, row);
        const char *name = duckdb_value_varchar(&result, 1, row);
        printf("%d %s\n", id, name);
    }

    // Clean up
    duckdb_destroy_result(&result);
    duckdb_disconnect(&con);
    duckdb_close(&db);

    return 0;
}
