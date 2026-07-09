// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashSet;
use std::sync::Arc;
use std::{collections::HashMap, fmt::Debug, hash::Hash};

use adbc_core::options::Statistics;
use arrow_array::{
    Float64Array, Int16Array, RecordBatch, RecordBatchReader, StringArray, UInt32Array,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};

use adbc_core::{
    Connection, Database, Driver, Optionable, PartitionedResult, Statement, constants,
    error::{Error, Result, Status},
    options::{
        InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue,
    },
    schemas,
    schemas::builder::{
        ColumnSchema, ConstraintUsage, GetInfoBuilder, GetObjectsBuilder, GetStatisticsBuilder,
        StatisticValue, TableConstraint,
    },
};

#[derive(Debug)]
pub struct SingleBatchReader {
    batch: Option<RecordBatch>,
    schema: SchemaRef,
}

impl SingleBatchReader {
    pub fn new(batch: RecordBatch) -> Self {
        let schema = batch.schema();
        Self {
            batch: Some(batch),
            schema,
        }
    }
}

impl Iterator for SingleBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        Ok(self.batch.take()).transpose()
    }
}

impl RecordBatchReader for SingleBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn get_table_schema() -> Schema {
    Schema::new(vec![
        Field::new("a", DataType::UInt32, true),
        Field::new("b", DataType::Float64, false),
        Field::new("c", DataType::Utf8, true),
    ])
}

fn get_table_data() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(get_table_schema()),
        vec![
            Arc::new(UInt32Array::from(vec![1, 2, 3])),
            Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5])),
            Arc::new(StringArray::from(vec!["A", "B", "C"])),
        ],
    )
    .unwrap()
}

fn set_option<T>(options: &mut HashMap<T, OptionValue>, key: T, value: OptionValue) -> Result<()>
where
    T: Eq + Hash,
{
    options.insert(key, value);
    Ok(())
}

fn get_option_bytes<T>(options: &HashMap<T, OptionValue>, key: T, kind: &str) -> Result<Vec<u8>>
where
    T: Eq + Hash + Debug,
{
    let value = options.get(&key);
    match value {
        None => Err(Error::with_message_and_status(
            format!("Unrecognized {kind} option: {key:?}"),
            Status::NotFound,
        )),
        Some(value) => match value {
            OptionValue::Bytes(value) => Ok(value.clone()),
            _ => Err(Error::with_message_and_status(
                format!("Incorrect value for {kind} option: {key:?}"),
                Status::InvalidData,
            )),
        },
    }
}

fn get_option_double<T>(options: &HashMap<T, OptionValue>, key: T, kind: &str) -> Result<f64>
where
    T: Eq + Hash + Debug,
{
    let value = options.get(&key);
    match value {
        None => Err(Error::with_message_and_status(
            format!("Unrecognized {kind} option: {key:?}"),
            Status::NotFound,
        )),
        Some(value) => match value {
            OptionValue::Double(value) => Ok(*value),
            _ => Err(Error::with_message_and_status(
                format!("Incorrect value for {kind} option: {key:?}"),
                Status::InvalidData,
            )),
        },
    }
}

fn get_option_int<T>(options: &HashMap<T, OptionValue>, key: T, kind: &str) -> Result<i64>
where
    T: Eq + Hash + Debug,
{
    let value = options.get(&key);
    match value {
        None => Err(Error::with_message_and_status(
            format!("Unrecognized {kind} option: {key:?}"),
            Status::NotFound,
        )),
        Some(value) => match value {
            OptionValue::Int(value) => Ok(*value),
            _ => Err(Error::with_message_and_status(
                format!("Incorrect value for {kind} option: {key:?}"),
                Status::InvalidData,
            )),
        },
    }
}

fn get_option_string<T>(options: &HashMap<T, OptionValue>, key: T, kind: &str) -> Result<String>
where
    T: Eq + Hash + Debug,
{
    let value = options.get(&key);
    match value {
        None => Err(Error::with_message_and_status(
            format!("Unrecognized {kind} option: {key:?}"),
            Status::NotFound,
        )),
        Some(value) => match value {
            OptionValue::String(value) => Ok(value.clone()),
            _ => Err(Error::with_message_and_status(
                format!("Incorrect value for {kind} option: {key:?}"),
                Status::InvalidData,
            )),
        },
    }
}

fn maybe_panic(fnname: impl AsRef<str>) {
    if let Some(func) = std::env::var_os("PANICDUMMY_FUNC").map(|x| x.to_string_lossy().to_string())
    {
        if fnname.as_ref() == func {
            let message = std::env::var_os("PANICDUMMY_MESSAGE")
                .map(|x| x.to_string_lossy().to_string())
                .unwrap_or_else(|| format!("We panicked in {}!", fnname.as_ref()));
            panic!("{}", message);
        }
    }
}

/// A dummy driver used for testing purposes.
#[derive(Default)]
pub struct DummyDriver {}

impl Driver for DummyDriver {
    type DatabaseType = DummyDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        Ok(Self::DatabaseType::default())
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (<Self::DatabaseType as Optionable>::Option, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut database = Self::DatabaseType::default();
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        Ok(database)
    }
}

#[derive(Default)]
pub struct DummyDatabase {
    options: HashMap<OptionDatabase, OptionValue>,
}

impl Optionable for DummyDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        set_option(&mut self.options, key, value)
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        get_option_bytes(&self.options, key, "database")
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        get_option_double(&self.options, key, "database")
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        get_option_int(&self.options, key, "database")
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        get_option_string(&self.options, key, "database")
    }
}

impl Database for DummyDatabase {
    type ConnectionType = DummyConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        Ok(Self::ConnectionType::default())
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (<Self::ConnectionType as Optionable>::Option, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let mut connection = Self::ConnectionType::default();
        for (key, value) in opts {
            connection.set_option(key, value)?;
        }
        Ok(connection)
    }
}

#[derive(Default)]
pub struct DummyConnection {
    options: HashMap<OptionConnection, OptionValue>,
}

impl Optionable for DummyConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        set_option(&mut self.options, key, value)
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        get_option_bytes(&self.options, key, "connection")
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        get_option_double(&self.options, key, "connection")
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        get_option_int(&self.options, key, "connection")
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        get_option_string(&self.options, key, "connection")
    }
}

impl Connection for DummyConnection {
    type StatementType = DummyStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Ok(Self::StatementType::default())
    }

    // This method is used to test that errors round-trip correctly.
    fn cancel(&mut self) -> Result<()> {
        let mut error = Error::with_message_and_status("message", Status::Cancelled);
        error.vendor_code = constants::ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA;
        error.sqlstate = [1, 2, 3, 4, 5];
        error.details = Some(vec![
            ("key1".into(), b"AAA".into()),
            ("key2".into(), b"ZZZZZ".into()),
        ]);
        Err(error)
    }

    fn commit(&mut self) -> Result<()> {
        Ok(())
    }

    fn get_info(
        &self,
        _codes: Option<HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let mut builder = GetInfoBuilder::new();
        builder
            .append_string(InfoCode::VendorName, "MyVendorName")
            .append_bool(InfoCode::VendorVersion, true)
            .append_int64(InfoCode::VendorArrowVersion, 42)
            .append_int32_bitmask(InfoCode::DriverName, 1337)
            .append_string_list(InfoCode::DriverVersion, ["Hello", "World"])
            .append_int32_to_int32_list_map(
                InfoCode::DriverArrowVersion,
                [(42, [1, 2, 3]), (1337, [1, 4, 9])],
            );
        let reader = SingleBatchReader::new(builder.finish()?);
        Ok(Box::new(reader))
    }

    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let mut builder = GetObjectsBuilder::new(ObjectDepth::All);
        builder.append_catalog(Some("default"));
        builder.append_db_schema(Some("default"));
        builder.append_table("default", "table");
        builder.append_column(ColumnSchema {
            column_name: "my_column".into(),
            ordinal_position: Some(0),
            remarks: Some("Nice column!".into()),
            xdbc_data_type: Some(0),
            xdbc_type_name: Some("my_type".into()),
            xdbc_column_size: Some(42),
            xdbc_decimal_digits: Some(42),
            xdbc_num_prec_radix: Some(42),
            xdbc_nullable: Some(42),
            xdbc_column_def: Some("column_def".into()),
            xdbc_sql_data_type: Some(42),
            xdbc_datetime_sub: Some(42),
            xdbc_char_octet_length: Some(42),
            xdbc_is_nullable: Some("YES".into()),
            xdbc_scope_catalog: Some("MyCatalog".into()),
            xdbc_scope_schema: Some("MySchema".into()),
            xdbc_scope_table: Some("MyTable".into()),
            xdbc_is_autoincrement: Some(true),
            xdbc_is_generatedcolumn: Some(true),
        });
        builder.append_constraint(TableConstraint {
            constraint_name: Some("my_constraint".into()),
            constraint_type: "FOREIGN KEY".into(),
            constraint_column_names: vec!["my_other_column".into()],
            constraint_column_usage: vec![ConstraintUsage {
                fk_catalog: Some("my_catalog".into()),
                fk_db_schema: Some("my_db_schema".into()),
                fk_table: "my_table".into(),
                fk_column_name: "my_column".into(),
            }],
        });
        let reader = SingleBatchReader::new(builder.finish()?);
        Ok(Box::new(reader))
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let mut builder = GetStatisticsBuilder::new();
        builder.append_catalog(Some("default"));
        builder.append_db_schema(Some("default"));
        builder.append_statistic(
            "default",
            Some("my_column"),
            Statistics::AverageByteWidth,
            StatisticValue::UInt64(42),
            false,
        );
        let reader = SingleBatchReader::new(builder.finish()?);
        Ok(Box::new(reader))
    }

    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let name_array = StringArray::from(vec!["sum", "min", "max"]);
        let key_array = Int16Array::from(vec![0, 1, 2]);
        let batch = RecordBatch::try_new(
            schemas::GET_STATISTIC_NAMES_SCHEMA.clone(),
            vec![Arc::new(name_array), Arc::new(key_array)],
        )?;
        let reader = SingleBatchReader::new(batch);
        Ok(Box::new(reader))
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<arrow_schema::Schema> {
        let catalog = catalog.unwrap_or("default");
        let db_schema = db_schema.unwrap_or("default");

        if catalog == "default" && db_schema == "default" && table_name == "default" {
            Ok(get_table_schema())
        } else {
            Err(Error::with_message_and_status(
                format!("Table {catalog}.{db_schema}.{table_name} does not exist"),
                Status::NotFound,
            ))
        }
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let array = Arc::new(StringArray::from(vec!["table", "view"]));
        let batch = RecordBatch::try_new(schemas::GET_TABLE_TYPES_SCHEMA.clone(), vec![array])?;
        let reader = SingleBatchReader::new(batch);
        Ok(Box::new(reader))
    }

    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let batch = get_table_data();
        let reader = SingleBatchReader::new(batch);
        Ok(Box::new(reader))
    }

    fn rollback(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Default)]
pub struct DummyStatement {
    options: HashMap<OptionStatement, OptionValue>,
}

impl Optionable for DummyStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        set_option(&mut self.options, key, value)
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        get_option_bytes(&self.options, key, "statement")
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        get_option_double(&self.options, key, "statement")
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        get_option_int(&self.options, key, "statement")
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        get_option_string(&self.options, key, "statement")
    }
}

impl Statement for DummyStatement {
    fn bind(&mut self, _batch: RecordBatch) -> Result<()> {
        Ok(())
    }

    fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        Ok(())
    }

    fn cancel(&mut self) -> Result<()> {
        Ok(())
    }

    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        maybe_panic("StatementExecuteQuery");
        let batch = get_table_data();
        let reader = SingleBatchReader::new(batch);
        Ok(Box::new(reader))
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        Ok(PartitionedResult {
            partitions: vec![b"AAA".into(), b"ZZZZZ".into()],
            schema: get_table_schema(),
            rows_affected: 0,
        })
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        Ok(get_table_schema())
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        Ok(Some(0))
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        Ok(get_table_schema())
    }

    fn prepare(&mut self) -> Result<()> {
        Ok(())
    }

    fn set_sql_query(&mut self, _query: impl AsRef<str>) -> Result<()> {
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Ok(())
    }
}

impl Drop for DummyStatement {
    fn drop(&mut self) {
        maybe_panic("StatementClose");
    }
}

adbc_ffi::export_driver!(AdbcDummyInit, DummyDriver);
