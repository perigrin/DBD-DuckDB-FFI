#include <duckdb.h>
#include <stdio.h>

int main() {
	duckdb_database db;
	duckdb_connection con;
	duckdb_open(NULL, &db);
	duckdb_connect(db, &con);

	duckdb_result res;
	duckdb_query(con, "CREATE TABLE integers (i INTEGER, j INTEGER);", NULL);
	duckdb_query(con, "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);", NULL);
	duckdb_query(con, "SELECT * FROM integers;", &res);

	// iterate until result is exhausted
	while (true) {
		duckdb_data_chunk result = duckdb_fetch_chunk(res);
		if (!result) {
			// result is exhausted
			break;
		}
		// get the number of rows from the data chunk
		idx_t row_count = duckdb_data_chunk_get_size(result);
		// get the first column
		duckdb_vector col1 = duckdb_data_chunk_get_vector(result, 0);
		int32_t *col1_data = (int32_t *) duckdb_vector_get_data(col1);
		uint64_t *col1_validity = duckdb_vector_get_validity(col1);

		// get the second column
		duckdb_vector col2 = duckdb_data_chunk_get_vector(result, 1);
		int32_t *col2_data = (int32_t *) duckdb_vector_get_data(col2);
		uint64_t *col2_validity = duckdb_vector_get_validity(col2);

		// iterate over the rows
		for (idx_t row = 0; row < row_count; row++) {
			if (duckdb_validity_row_is_valid(col1_validity, row)) {
				printf("%d", col1_data[row]);
			} else {
				printf("NULL");
			}
			printf(",");
			if (duckdb_validity_row_is_valid(col2_validity, row)) {
				printf("%d", col2_data[row]);
			} else {
				printf("NULL");
			}
			printf("\n");
		}
		duckdb_destroy_data_chunk(&result);
	}
	// clean-up
	duckdb_destroy_result(&res);
	duckdb_disconnect(&con);
	duckdb_close(&db);
	return 0;
}
