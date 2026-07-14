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

//! Tests that `ConnectionCancel`/`StatementCancel` can be called from another
//! thread while a call is in flight on the same object, as the ADBC C API
//! requires ("this must always be thread-safe (other operations are not)").
//!
//! The test driver blocks inside a driver method until its cancel token is
//! invoked, so the cancel shim provably runs while the executing shim holds a
//! borrow of the driver object. Run under Miri to check for aliasing
//! violations.

use std::collections::HashSet;
use std::sync::{Arc, Condvar, LazyLock, Mutex};

use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, Schema};

use adbc_core::constants::{ADBC_STATUS_NOT_IMPLEMENTED, ADBC_STATUS_OK};
use adbc_core::error::Result;
use adbc_core::options::{
    InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue,
};
use adbc_core::{
    CancelToken, Connection, Database, Driver, Optionable, PartitionedResult, Statement,
};
use adbc_ffi::{FFI_AdbcConnection, FFI_AdbcDatabase, FFI_AdbcError, FFI_AdbcStatement, FFIDriver};

/// Blocks a driver method until the cancel token is invoked.
#[derive(Default)]
struct Blocker {
    state: Mutex<(bool, bool)>, // (entered, cancelled)
    condvar: Condvar,
}

impl Blocker {
    fn block_until_cancelled(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.condvar.notify_all();
        while !state.1 {
            state = self.condvar.wait(state).unwrap();
        }
    }

    fn wait_until_entered(&self) {
        let mut state = self.state.lock().unwrap();
        while !state.0 {
            state = self.condvar.wait(state).unwrap();
        }
    }
}

impl CancelToken for Blocker {
    fn cancel(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.1 = true;
        self.condvar.notify_all();
        Ok(())
    }
}

static CONNECTION_BLOCKER: LazyLock<Arc<Blocker>> = LazyLock::new(Arc::default);
static STATEMENT_BLOCKER: LazyLock<Arc<Blocker>> = LazyLock::new(Arc::default);

fn empty_reader() -> Box<dyn RecordBatchReader + Send + 'static> {
    let batches = std::iter::empty::<std::result::Result<RecordBatch, ArrowError>>();
    Box::new(RecordBatchIterator::new(batches, Arc::new(Schema::empty())))
}

#[derive(Default)]
struct TestDriver {}

impl Driver for TestDriver {
    type DatabaseType = TestDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        Ok(TestDatabase { tokenless: false })
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut tokenless = false;
        for (key, value) in opts {
            match (key, value) {
                (OptionDatabase::Other(key), OptionValue::String(value))
                    if key == "tokenless" && value == "true" =>
                {
                    tokenless = true;
                }
                other => panic!("unexpected database option: {other:?}"),
            }
        }
        Ok(TestDatabase { tokenless })
    }
}

/// `tokenless` makes every connection/statement return no cancel token, to
/// test that the exporter then reports cancellation as not implemented.
struct TestDatabase {
    tokenless: bool,
}

impl Optionable for TestDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, _key: Self::Option, _value: OptionValue) -> Result<()> {
        unimplemented!()
    }
    fn get_option_string(&self, _key: Self::Option) -> Result<String> {
        unimplemented!()
    }
    fn get_option_bytes(&self, _key: Self::Option) -> Result<Vec<u8>> {
        unimplemented!()
    }
    fn get_option_int(&self, _key: Self::Option) -> Result<i64> {
        unimplemented!()
    }
    fn get_option_double(&self, _key: Self::Option) -> Result<f64> {
        unimplemented!()
    }
}

impl Database for TestDatabase {
    type ConnectionType = TestConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        Ok(TestConnection {
            reads: std::cell::Cell::new(0),
            tokenless: self.tokenless,
        })
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        assert!(opts.into_iter().next().is_none());
        self.new_connection()
    }
}

struct TestConnection {
    reads: std::cell::Cell<u64>,
    tokenless: bool,
}

impl Optionable for TestConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, _key: Self::Option, _value: OptionValue) -> Result<()> {
        unimplemented!()
    }
    fn get_option_string(&self, _key: Self::Option) -> Result<String> {
        unimplemented!()
    }
    fn get_option_bytes(&self, _key: Self::Option) -> Result<Vec<u8>> {
        unimplemented!()
    }
    fn get_option_int(&self, _key: Self::Option) -> Result<i64> {
        unimplemented!()
    }
    fn get_option_double(&self, _key: Self::Option) -> Result<f64> {
        unimplemented!()
    }
}

impl Connection for TestConnection {
    type StatementType = TestStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Ok(TestStatement {
            updates: 0,
            tokenless: self.tokenless,
        })
    }

    fn cancel(&mut self) -> Result<()> {
        unimplemented!()
    }

    fn cancel_token(&mut self) -> Option<Arc<dyn CancelToken>> {
        (!self.tokenless).then(|| CONNECTION_BLOCKER.clone() as Arc<dyn CancelToken>)
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.reads.set(self.reads.get() + 1);
        CONNECTION_BLOCKER.block_until_cancelled();
        self.reads.set(self.reads.get() + 1);
        Ok(empty_reader())
    }

    fn get_info(
        &self,
        _codes: Option<HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        unimplemented!()
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
        unimplemented!()
    }
    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> Result<Schema> {
        unimplemented!()
    }
    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        unimplemented!()
    }
    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        unimplemented!()
    }
    fn commit(&mut self) -> Result<()> {
        unimplemented!()
    }
    fn rollback(&mut self) -> Result<()> {
        unimplemented!()
    }
    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        unimplemented!()
    }
}

struct TestStatement {
    updates: u64,
    tokenless: bool,
}

impl Optionable for TestStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, _key: Self::Option, _value: OptionValue) -> Result<()> {
        unimplemented!()
    }
    fn get_option_string(&self, _key: Self::Option) -> Result<String> {
        unimplemented!()
    }
    fn get_option_bytes(&self, _key: Self::Option) -> Result<Vec<u8>> {
        unimplemented!()
    }
    fn get_option_int(&self, _key: Self::Option) -> Result<i64> {
        unimplemented!()
    }
    fn get_option_double(&self, _key: Self::Option) -> Result<f64> {
        unimplemented!()
    }
}

impl Statement for TestStatement {
    fn cancel(&mut self) -> Result<()> {
        unimplemented!()
    }

    fn cancel_token(&mut self) -> Option<Arc<dyn CancelToken>> {
        (!self.tokenless).then(|| STATEMENT_BLOCKER.clone() as Arc<dyn CancelToken>)
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        self.updates += 1;
        STATEMENT_BLOCKER.block_until_cancelled();
        self.updates += 1;
        Ok(None)
    }

    fn bind(&mut self, _batch: RecordBatch) -> Result<()> {
        unimplemented!()
    }
    fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        unimplemented!()
    }
    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        unimplemented!()
    }
    fn execute_schema(&mut self) -> Result<Schema> {
        unimplemented!()
    }
    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        unimplemented!()
    }
    fn get_parameter_schema(&self) -> Result<Schema> {
        unimplemented!()
    }
    fn prepare(&mut self) -> Result<()> {
        unimplemented!()
    }
    fn set_sql_query(&mut self, _query: impl AsRef<str>) -> Result<()> {
        unimplemented!()
    }
    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        unimplemented!()
    }
}

struct SendPtr<T>(*mut T);
unsafe impl<T> Send for SendPtr<T> {}

impl<T> SendPtr<T> {
    // A method (not a field access) so closures capture the wrapper, not the pointer.
    fn get(&self) -> *mut T {
        self.0
    }
}

#[test]
fn cancel_during_execute_does_not_alias() {
    let driver = TestDriver::ffi_driver();
    let mut error = FFI_AdbcError::default();

    let mut database = FFI_AdbcDatabase::default();
    let database_ptr = &raw mut database;
    let mut connection = FFI_AdbcConnection::default();
    let connection_ptr = &raw mut connection;
    let mut statement = FFI_AdbcStatement::default();
    let statement_ptr = &raw mut statement;

    unsafe {
        assert_eq!(
            driver.DatabaseNew.unwrap()(database_ptr, &mut error),
            ADBC_STATUS_OK
        );
        assert_eq!(
            driver.DatabaseInit.unwrap()(database_ptr, &mut error),
            ADBC_STATUS_OK
        );
        assert_eq!(
            driver.ConnectionNew.unwrap()(connection_ptr, &mut error),
            ADBC_STATUS_OK
        );
        assert_eq!(
            driver.ConnectionInit.unwrap()(connection_ptr, database_ptr, &mut error),
            ADBC_STATUS_OK
        );
        assert_eq!(
            driver.StatementNew.unwrap()(connection_ptr, statement_ptr, &mut error),
            ADBC_STATUS_OK
        );
    }

    // Cancel a statement while StatementExecuteQuery is blocked inside it.
    let execute_query = driver.StatementExecuteQuery.unwrap();
    let sendable = SendPtr(statement_ptr);
    let executor = std::thread::spawn(move || {
        let statement_ptr = sendable.get();
        let mut error = FFI_AdbcError::default();
        unsafe {
            execute_query(
                statement_ptr,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut error,
            )
        }
    });
    STATEMENT_BLOCKER.wait_until_entered();
    unsafe {
        assert_eq!(
            driver.StatementCancel.unwrap()(statement_ptr, &mut error),
            ADBC_STATUS_OK
        );
    }
    assert_eq!(executor.join().unwrap(), ADBC_STATUS_OK);

    // Cancel a connection while ConnectionGetTableTypes is blocked inside it.
    let get_table_types = driver.ConnectionGetTableTypes.unwrap();
    let sendable = SendPtr(connection_ptr);
    let executor = std::thread::spawn(move || {
        let connection_ptr = sendable.get();
        let mut stream = arrow_array::ffi_stream::FFI_ArrowArrayStream::empty();
        let mut error = FFI_AdbcError::default();
        unsafe { get_table_types(connection_ptr, &mut stream, &mut error) }
    });
    CONNECTION_BLOCKER.wait_until_entered();
    unsafe {
        assert_eq!(
            driver.ConnectionCancel.unwrap()(connection_ptr, &mut error),
            ADBC_STATUS_OK
        );
    }
    assert_eq!(executor.join().unwrap(), ADBC_STATUS_OK);

    unsafe {
        assert_eq!(
            driver.StatementRelease.unwrap()(statement_ptr, &mut error),
            ADBC_STATUS_OK
        );
        assert_eq!(
            driver.ConnectionRelease.unwrap()(connection_ptr, &mut error),
            ADBC_STATUS_OK
        );
        assert_eq!(
            driver.DatabaseRelease.unwrap()(database_ptr, &mut error),
            ADBC_STATUS_OK
        );
    }
}

/// A driver whose `cancel_token` returns `None` gets NOT_IMPLEMENTED from the
/// cancel shims — there is no fallback to the (unsound-under-concurrency)
/// `cancel(&mut self)` trait methods.
#[test]
fn cancel_without_token_reports_not_implemented() {
    let driver = TestDriver::ffi_driver();
    let mut error = FFI_AdbcError::default();

    let mut database = FFI_AdbcDatabase::default();
    let database_ptr = &raw mut database;
    let mut connection = FFI_AdbcConnection::default();
    let connection_ptr = &raw mut connection;
    let mut statement = FFI_AdbcStatement::default();
    let statement_ptr = &raw mut statement;

    unsafe {
        assert_eq!(
            driver.DatabaseNew.unwrap()(database_ptr, &mut error),
            ADBC_STATUS_OK
        );
        assert_eq!(
            driver.DatabaseSetOption.unwrap()(
                database_ptr,
                c"tokenless".as_ptr(),
                c"true".as_ptr(),
                &mut error
            ),
            ADBC_STATUS_OK
        );
        assert_eq!(
            driver.DatabaseInit.unwrap()(database_ptr, &mut error),
            ADBC_STATUS_OK
        );
        assert_eq!(
            driver.ConnectionNew.unwrap()(connection_ptr, &mut error),
            ADBC_STATUS_OK
        );
        assert_eq!(
            driver.ConnectionInit.unwrap()(connection_ptr, database_ptr, &mut error),
            ADBC_STATUS_OK
        );
        assert_eq!(
            driver.StatementNew.unwrap()(connection_ptr, statement_ptr, &mut error),
            ADBC_STATUS_OK
        );

        let mut error = FFI_AdbcError::default();
        assert_eq!(
            driver.StatementCancel.unwrap()(statement_ptr, &mut error),
            ADBC_STATUS_NOT_IMPLEMENTED
        );
        let mut error = FFI_AdbcError::default();
        assert_eq!(
            driver.ConnectionCancel.unwrap()(connection_ptr, &mut error),
            ADBC_STATUS_NOT_IMPLEMENTED
        );

        assert_eq!(
            driver.StatementRelease.unwrap()(statement_ptr, &mut error),
            ADBC_STATUS_OK
        );
        assert_eq!(
            driver.ConnectionRelease.unwrap()(connection_ptr, &mut error),
            ADBC_STATUS_OK
        );
        assert_eq!(
            driver.DatabaseRelease.unwrap()(database_ptr, &mut error),
            ADBC_STATUS_OK
        );
    }
}
