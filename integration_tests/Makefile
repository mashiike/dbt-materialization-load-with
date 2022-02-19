
test-redshift-upload:
	aws s3 cp seeds/table_expected.csv s3://${S3_BUCKET_NAME}/${S3_KEY_PREFIX}test_load_table.csv
	aws s3 cp seeds/incremental_seed.csv s3://${S3_BUCKET_NAME}/${S3_KEY_PREFIX}test_load_incremental.csv

test-redshift:
	dbt deps
	dbt seed --target redshift --full-refresh
	dbt run --target redshift --full-refresh
	LOAD_INCREMENTAL_TARGET=test_load_incremental.csv dbt run --target redshift
	dbt test --target redshift

clean:
	dbt clean
