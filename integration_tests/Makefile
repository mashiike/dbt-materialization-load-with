
test-redshift-upload:
	aws s3 cp seeds/expected.csv s3://${S3_BUCKET_NAME}/${S3_KEY_PREFIX}test_load_table.csv
test-redshift:
	dbt deps
	dbt seed --target redshift --full-refresh
	dbt run --target redshift --full-refresh
	dbt test --target redshift
