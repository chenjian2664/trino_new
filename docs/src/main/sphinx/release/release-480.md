# Release 480 (dd MMM 2025)

## General
* {{breaking}} `enable-large-dynamic-filters` configuration property and the corresponding system 
  session property `enable_large_dynamic_filters` has been removed. Large dynamic filters are used 
  by default. ({issue}`27637`)
* {{breaking}} `dynamic-filtering.small*` configuration properties are now defunct and must be removed
  from server configurations. ({issue}`27637`)
* {{breaking}} The previously deprecated `dynamic-filtering.large-broadcast*` configuration properties 
  are now defunct and must be removed from server configurations. ({issue}`27637`).
* `dynamic-filtering.large*` configuration properties have been deprecated in favor of `dynamic-filtering.*`. ({issue}`27637`)
* Extend experimental performance improvements for remote data exchanges on newer CPU architectures ({issue}`27586`)
* Enable experimental performance improvements for remote data exchanges on Graviton 4 CPUs ({issue}`27586`)
* Improve performance of queries with data exchanges or aggregations. ({issue}`27657`)

## Security

## Web UI

* Fix numeric ordering of stages in the UI. ({issue}`27655`)

## JDBC driver

## Docker image

## CLI

## BigQuery connector

## Blackhole connector

## Cassandra connector

## ClickHouse connector

* Drop the table in CTAS scenario in case of an exception during data insertion phase. ({issue}`27702`)

## Delta Lake connector

* {{breaking}} Remove live files table metadata cache. The configuration properties 
  `metadata.live-files.cache-size`, `metadata.live-files.cache-ttl` and `checkpoint-filtering.enabled` 
  are now defunct and must be removed from server configurations. ({issue}`27618`)
* {{breaking}} `hive.write-validation-threads` configuration property has been removed. ({issue}`27729`)
* {{breaking}} The legacy configuration property `parquet.optimized-writer.validation-percentage` is now defunct and must be migrated to `parquet.writer.validation-percentage` in server configurations. ({issue}`27729`)
* {{breaking}} The legacy configuration property `hive.parquet.writer.block-size` is now defunct and must be migrated to `parquet.writer.block-size` in server configurations. ({issue}`27729`)
* {{breaking}} The legacy configuration property `hive.parquet.writer.page-size` is now defunct and must be migrated to `parquet.writer.page-size` in server configurations. ({issue}`27729`)
* Improve effectiveness of bloom filters written in parquet files for high cardinality columns. ({issue}`27656`)

## Druid connector

## DuckDB connector

* Drop the table in CTAS scenario in case of an exception during data insertion phase. ({issue}`27702`)

## Elasticsearch connector

## Exasol connector

## Faker connector

## Google Sheets connector

## Hive connector

* {{breaking}} `hive.write-validation-threads` configuration property has been removed. ({issue}`27729`)
* {{breaking}} The legacy configuration property `parquet.optimized-writer.validation-percentage` is now defunct and must be migrated to `parquet.writer.validation-percentage` in server configurations. ({issue}`27729`)
* {{breaking}} The legacy configuration property `hive.parquet.writer.block-size` is now defunct and must be migrated to `parquet.writer.block-size` in server configurations. ({issue}`27729`)
* {{breaking}} The legacy configuration property `hive.parquet.writer.page-size` is now defunct and must be migrated to `parquet.writer.page-size` in server configurations. ({issue}`27729`)
* Improve effectiveness of bloom filters written in parquet files for high cardinality columns. ({issue}`27656`)

## Hudi connector

* {{breaking}} `hive.write-validation-threads` configuration property has been removed. ({issue}`27729`)
* {{breaking}} The legacy configuration property `parquet.optimized-writer.validation-percentage` is now defunct and must be migrated to `parquet.writer.validation-percentage` in server configurations. ({issue}`27729`)
* {{breaking}} The legacy configuration property `hive.parquet.writer.block-size` is now defunct and must be migrated to `parquet.writer.block-size` in server configurations. ({issue}`27729`)
* {{breaking}} The legacy configuration property `hive.parquet.writer.page-size` is now defunct and must be migrated to `parquet.writer.page-size` in server configurations. ({issue}`27729`)
* Improve effectiveness of bloom filters written in parquet files for high cardinality columns. ({issue}`27656`)

## Iceberg connector

* Add support for BigLake metastore in Iceberg REST catalog. ({issue}`26219`)
* Add `delete_after_commit_enabled` and `max_previous_versions` table properties. ({issue}`14128`)
* {{breaking}} `hive.write-validation-threads` configuration property has been removed. ({issue}`27729`)
* {{breaking}} The legacy configuration property `parquet.optimized-writer.validation-percentage` is now defunct and must be migrated to `parquet.writer.validation-percentage` in server configurations. ({issue}`27729`)
* {{breaking}} The legacy configuration property `hive.parquet.writer.block-size` is now defunct and must be migrated to `parquet.writer.block-size` in server configurations. ({issue}`27729`)
* {{breaking}} The legacy configuration property `hive.parquet.writer.page-size` is now defunct and must be migrated to `parquet.writer.page-size` in server configurations. ({issue}`27729`)
* Improve effectiveness of bloom filters written in parquet files for high cardinality columns. ({issue}`27656`)
* Fix failure when reading `$files` metadata table with partition evolution using `truncate` or `bucket` 
  on the same column. ({issue}`26109`)

## Ignite connector

* Drop the table in CTAS scenario in case of an exception during data insertion phase. ({issue}`27702`)

## JMX connector

## Kafka connector

## Lakehouse

* Fix failure when reading Iceberg `$files` table. ({issue}`26751`)

## Loki connector

## MariaDB connector

* Drop the table in CTAS scenario in case of an exception during data insertion phase. ({issue}`27702`)

## Memory connector

## MongoDB connector

## MySQL connector

* Drop the table in CTAS scenario in case of an exception during data insertion phase. ({issue}`27702`)

## OpenSearch connector

## Oracle connector

* Drop the table in CTAS scenario in case of an exception during data insertion phase. ({issue}`27702`)

## Pinot connector

## PostgreSQL connector

* Drop the table in CTAS scenario in case of an exception during data insertion phase. ({issue}`27702`)

## Prometheus connector

## Redis connector

## Redshift connector

* Drop the table in CTAS scenario in case of an exception during data insertion phase. ({issue}`27702`)

## SingleStore connector

* Drop the table in CTAS scenario in case of an exception during data insertion phase. ({issue}`27702`)

## Snowflake connector

* Drop the table in CTAS scenario in case of an exception during data insertion phase. ({issue}`27702`)

## SQL Server connector

* Drop the table in CTAS scenario in case of an exception during data insertion phase. ({issue}`27702`)

## TPC-H connector

## TPC-DS connector

## Vertica connector

## SPI

* Remove support for `TypeSignatureParameter`. Use `TypeParameter`, instead. ({issue}`27574`)
* Remove support for `ParameterKind`. Use `TypeParameter.Type`, `TypeParameter.Numeric`, `TypeParameter.Variable`, instead. ({issue}`27574`)
* Remove support for `NamedType`, `NamedTypeSignature` and `NamedTypeParameter`. Use `TypeParameter.Type`, instead. ({issue}`27574`)