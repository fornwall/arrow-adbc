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

//! Builders for the nested [`RecordBatch`] shapes returned by
//! [get_info][crate::Connection::get_info],
//! [get_objects][crate::Connection::get_objects], and
//! [get_statistics][crate::Connection::get_statistics].
//!
//! These result sets are described by union/list/struct schemas (see the
//! [parent module][crate::schemas]) that are tedious and error-prone to assemble
//! by hand. The builders here own the union type-ids, list offsets, and null
//! handling so drivers can append typed values and call
//! [`finish`][GetInfoBuilder::finish] to obtain a schema-conformant batch.

use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Float64Builder, Int32Builder, Int64Builder,
    ListBuilder, MapBuilder, MapFieldNames, StringBuilder, UInt32Builder, UInt64Builder,
};
use arrow_array::{
    ArrayRef, BooleanArray, Int16Array, Int32Array, ListArray, RecordBatch, StringArray,
    StructArray, UnionArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields, UnionFields};

use crate::error::Result;
use crate::options::{InfoCode, ObjectDepth};
use crate::schemas;

/// Extract the [`UnionFields`] from a [`DataType::Union`], panicking otherwise.
fn union_fields(data_type: &DataType) -> UnionFields {
    match data_type {
        DataType::Union(fields, _) => fields.clone(),
        other => unreachable!("expected a union type, got {other:?}"),
    }
}

/// Extract the [`Fields`] from a [`DataType::Struct`], panicking otherwise.
fn struct_fields(data_type: &DataType) -> Fields {
    match data_type {
        DataType::Struct(fields) => fields.clone(),
        other => unreachable!("expected a struct type, got {other:?}"),
    }
}

/// Build a list offset buffer from per-parent element counts.
///
/// Given `[2, 0, 1]` (parent 0 has 2 children, parent 1 has none, parent 2 has
/// one) this yields the offsets `[0, 2, 2, 3]`.
fn offsets_from_counts(counts: &[i32]) -> OffsetBuffer<i32> {
    let offsets: Vec<i32> = std::iter::once(0)
        .chain(counts.iter().scan(0, |acc, &count| {
            *acc += count;
            Some(*acc)
        }))
        .collect();
    OffsetBuffer::new(ScalarBuffer::from(offsets))
}

/// A list item field named `"item"`, matching the schemas in [`crate::schemas`].
fn list_item(data_type: DataType) -> Arc<Field> {
    Arc::new(Field::new("item", data_type, true))
}

/// Builder for the result of [get_info][crate::Connection::get_info].
///
/// Each `append_*` call adds one row, associating an [`InfoCode`] with a value
/// of the matching union variant.
///
/// ```
/// use adbc_core::options::InfoCode;
/// use adbc_core::schemas::builder::GetInfoBuilder;
///
/// let mut builder = GetInfoBuilder::default();
/// builder
///     .append_string(InfoCode::VendorName, "MyVendor")
///     .append_bool(InfoCode::VendorSql, true);
/// let batch = builder.finish().unwrap();
/// assert_eq!(batch.num_rows(), 2);
/// ```
pub struct GetInfoBuilder {
    names: UInt32Builder,
    type_ids: Vec<i8>,
    offsets: Vec<i32>,
    string_values: StringBuilder,
    bool_values: BooleanBuilder,
    int64_values: Int64Builder,
    int32_values: Int32Builder,
    string_list_values: ListBuilder<StringBuilder>,
    map_values: MapBuilder<Int32Builder, ListBuilder<Int32Builder>>,
}

impl Default for GetInfoBuilder {
    fn default() -> Self {
        Self {
            names: UInt32Builder::new(),
            type_ids: Vec::new(),
            offsets: Vec::new(),
            string_values: StringBuilder::new(),
            bool_values: BooleanBuilder::new(),
            int64_values: Int64Builder::new(),
            int32_values: Int32Builder::new(),
            string_list_values: ListBuilder::new(StringBuilder::new()),
            map_values: MapBuilder::new(
                Some(MapFieldNames {
                    entry: "entries".to_string(),
                    key: "key".to_string(),
                    value: "value".to_string(),
                }),
                Int32Builder::new(),
                ListBuilder::new(Int32Builder::new()),
            ),
        }
    }
}

impl GetInfoBuilder {
    /// Create an empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    fn push(&mut self, code: InfoCode, type_id: i8, offset: i32) {
        self.names.append_value(u32::from(&code));
        self.type_ids.push(type_id);
        self.offsets.push(offset);
    }

    /// Append a `string_value` (union variant 0).
    pub fn append_string(&mut self, code: InfoCode, value: &str) -> &mut Self {
        self.push(code, 0, self.string_values.len() as i32);
        self.string_values.append_value(value);
        self
    }

    /// Append a `bool_value` (union variant 1).
    pub fn append_bool(&mut self, code: InfoCode, value: bool) -> &mut Self {
        self.push(code, 1, self.bool_values.len() as i32);
        self.bool_values.append_value(value);
        self
    }

    /// Append an `int64_value` (union variant 2).
    pub fn append_int64(&mut self, code: InfoCode, value: i64) -> &mut Self {
        self.push(code, 2, self.int64_values.len() as i32);
        self.int64_values.append_value(value);
        self
    }

    /// Append an `int32_bitmask` (union variant 3).
    pub fn append_int32_bitmask(&mut self, code: InfoCode, value: i32) -> &mut Self {
        self.push(code, 3, self.int32_values.len() as i32);
        self.int32_values.append_value(value);
        self
    }

    /// Append a `string_list` (union variant 4).
    pub fn append_string_list<I, S>(&mut self, code: InfoCode, values: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.push(code, 4, self.string_list_values.len() as i32);
        let items = self.string_list_values.values();
        for value in values {
            items.append_value(value.as_ref());
        }
        self.string_list_values.append(true);
        self
    }

    /// Append an `int32_to_int32_list_map` (union variant 5).
    pub fn append_int32_to_int32_list_map<I, L>(&mut self, code: InfoCode, entries: I) -> &mut Self
    where
        I: IntoIterator<Item = (i32, L)>,
        L: IntoIterator<Item = i32>,
    {
        self.push(code, 5, self.map_values.len() as i32);
        for (key, values) in entries {
            self.map_values.keys().append_value(key);
            let value_list = self.map_values.values();
            for value in values {
                value_list.values().append_value(value);
            }
            value_list.append(true);
        }
        self.map_values
            .append(true)
            .expect("map key and value builders are kept in sync");
        self
    }

    /// Finish building and produce a batch conforming to
    /// [`GET_INFO_SCHEMA`][schemas::GET_INFO_SCHEMA].
    pub fn finish(mut self) -> Result<RecordBatch> {
        let fields = union_fields(
            schemas::GET_INFO_SCHEMA
                .field_with_name("info_value")
                .expect("info_value field")
                .data_type(),
        );
        let values = UnionArray::try_new(
            fields,
            ScalarBuffer::from(self.type_ids),
            Some(ScalarBuffer::from(self.offsets)),
            vec![
                Arc::new(self.string_values.finish()),
                Arc::new(self.bool_values.finish()),
                Arc::new(self.int64_values.finish()),
                Arc::new(self.int32_values.finish()),
                Arc::new(self.string_list_values.finish()),
                Arc::new(self.map_values.finish()),
            ],
        )?;
        Ok(RecordBatch::try_new(
            schemas::GET_INFO_SCHEMA.clone(),
            vec![Arc::new(self.names.finish()), Arc::new(values)],
        )?)
    }
}

/// A single column of a table, as returned by
/// [get_objects][crate::Connection::get_objects].
///
/// Only [`column_name`][ColumnSchema::column_name] is required; the remaining
/// `xdbc_*` fields default to null. Use [`ColumnSchema::new`] and set the
/// fields you have.
#[derive(Debug, Clone, Default)]
pub struct ColumnSchema {
    pub column_name: String,
    pub ordinal_position: Option<i32>,
    pub remarks: Option<String>,
    pub xdbc_data_type: Option<i16>,
    pub xdbc_type_name: Option<String>,
    pub xdbc_column_size: Option<i32>,
    pub xdbc_decimal_digits: Option<i16>,
    pub xdbc_num_prec_radix: Option<i16>,
    pub xdbc_nullable: Option<i16>,
    pub xdbc_column_def: Option<String>,
    pub xdbc_sql_data_type: Option<i16>,
    pub xdbc_datetime_sub: Option<i16>,
    pub xdbc_char_octet_length: Option<i32>,
    pub xdbc_is_nullable: Option<String>,
    pub xdbc_scope_catalog: Option<String>,
    pub xdbc_scope_schema: Option<String>,
    pub xdbc_scope_table: Option<String>,
    pub xdbc_is_autoincrement: Option<bool>,
    pub xdbc_is_generatedcolumn: Option<bool>,
}

impl ColumnSchema {
    /// A column with the given name and all `xdbc_*` fields set to null.
    pub fn new(column_name: impl Into<String>) -> Self {
        Self {
            column_name: column_name.into(),
            ..Default::default()
        }
    }
}

/// A single column-usage entry of a constraint (e.g. the foreign key target).
#[derive(Debug, Clone, Default)]
pub struct ConstraintUsage {
    pub fk_catalog: Option<String>,
    pub fk_db_schema: Option<String>,
    pub fk_table: String,
    pub fk_column_name: String,
}

/// A single constraint of a table, as returned by
/// [get_objects][crate::Connection::get_objects].
#[derive(Debug, Clone, Default)]
pub struct TableConstraint {
    pub constraint_name: Option<String>,
    pub constraint_type: String,
    pub constraint_column_names: Vec<String>,
    pub constraint_column_usage: Vec<ConstraintUsage>,
}

/// Builder for the result of [get_objects][crate::Connection::get_objects].
///
/// The nested catalog → schema → table → column/constraint hierarchy is built
/// by appending in order: call [`append_catalog`][GetObjectsBuilder::append_catalog],
/// then [`append_db_schema`][GetObjectsBuilder::append_db_schema] for each of its
/// schemas, then [`append_table`][GetObjectsBuilder::append_table] for each of
/// their tables, and finally [`append_column`][GetObjectsBuilder::append_column] /
/// [`append_constraint`][GetObjectsBuilder::append_constraint] for that table.
///
/// The [`ObjectDepth`] passed to [`GetObjectsBuilder::new`] controls how much of
/// the hierarchy is materialized in the output: levels below the requested depth
/// are emitted as null lists.
pub struct GetObjectsBuilder {
    depth: ObjectDepth,
    catalog_names: Vec<Option<String>>,
    catalog_schema_counts: Vec<i32>,
    db_schema_names: Vec<Option<String>>,
    schema_table_counts: Vec<i32>,
    table_names: Vec<String>,
    table_types: Vec<String>,
    table_column_counts: Vec<i32>,
    table_constraint_counts: Vec<i32>,
    columns: Vec<ColumnSchema>,
    constraints: Vec<TableConstraint>,
}

impl GetObjectsBuilder {
    /// Create an empty builder for the given output [`ObjectDepth`].
    pub fn new(depth: ObjectDepth) -> Self {
        Self {
            depth,
            catalog_names: Vec::new(),
            catalog_schema_counts: Vec::new(),
            db_schema_names: Vec::new(),
            schema_table_counts: Vec::new(),
            table_names: Vec::new(),
            table_types: Vec::new(),
            table_column_counts: Vec::new(),
            table_constraint_counts: Vec::new(),
            columns: Vec::new(),
            constraints: Vec::new(),
        }
    }

    /// Begin a new catalog.
    pub fn append_catalog(&mut self, name: Option<&str>) -> &mut Self {
        self.catalog_names.push(name.map(String::from));
        self.catalog_schema_counts.push(0);
        self
    }

    /// Begin a new schema within the most recently appended catalog.
    pub fn append_db_schema(&mut self, name: Option<&str>) -> &mut Self {
        self.db_schema_names.push(name.map(String::from));
        self.schema_table_counts.push(0);
        *self
            .catalog_schema_counts
            .last_mut()
            .expect("append_catalog must be called before append_db_schema") += 1;
        self
    }

    /// Begin a new table within the most recently appended schema.
    pub fn append_table(&mut self, table_name: &str, table_type: &str) -> &mut Self {
        self.table_names.push(table_name.to_string());
        self.table_types.push(table_type.to_string());
        self.table_column_counts.push(0);
        self.table_constraint_counts.push(0);
        *self
            .schema_table_counts
            .last_mut()
            .expect("append_db_schema must be called before append_table") += 1;
        self
    }

    /// Add a column to the most recently appended table.
    pub fn append_column(&mut self, column: ColumnSchema) -> &mut Self {
        self.columns.push(column);
        *self
            .table_column_counts
            .last_mut()
            .expect("append_table must be called before append_column") += 1;
        self
    }

    /// Add a constraint to the most recently appended table.
    pub fn append_constraint(&mut self, constraint: TableConstraint) -> &mut Self {
        self.constraints.push(constraint);
        *self
            .table_constraint_counts
            .last_mut()
            .expect("append_table must be called before append_constraint") += 1;
        self
    }

    /// Finish building and produce a batch conforming to
    /// [`GET_OBJECTS_SCHEMA`][schemas::GET_OBJECTS_SCHEMA].
    pub fn finish(self) -> Result<RecordBatch> {
        let num_catalogs = self.catalog_names.len();
        let num_schemas = self.db_schema_names.len();
        let num_tables = self.table_names.len();

        let include_schemas = !matches!(self.depth, ObjectDepth::Catalogs);
        let include_tables = matches!(
            self.depth,
            ObjectDepth::Tables | ObjectDepth::Columns | ObjectDepth::All
        );
        let include_columns = matches!(self.depth, ObjectDepth::Columns | ObjectDepth::All);

        let column_item = list_item(schemas::COLUMN_SCHEMA.clone());
        let constraint_item = list_item(schemas::CONSTRAINT_SCHEMA.clone());
        let table_item = list_item(schemas::TABLE_SCHEMA.clone());
        let db_schema_item = list_item(schemas::OBJECTS_DB_SCHEMA_SCHEMA.clone());

        let catalog_db_schemas = if include_schemas {
            let db_schema_tables = if include_tables {
                let table_columns = if include_columns {
                    ListArray::new(
                        column_item,
                        offsets_from_counts(&self.table_column_counts),
                        build_columns_struct(&self.columns),
                        None,
                    )
                } else {
                    ListArray::new_null(column_item, num_tables)
                };

                let table_constraints = if self.constraints.is_empty() {
                    ListArray::new_null(constraint_item, num_tables)
                } else {
                    ListArray::new(
                        constraint_item,
                        offsets_from_counts(&self.table_constraint_counts),
                        build_constraints_struct(&self.constraints),
                        None,
                    )
                };

                let tables_struct = StructArray::new(
                    struct_fields(&schemas::TABLE_SCHEMA),
                    vec![
                        Arc::new(StringArray::from(self.table_names)) as ArrayRef,
                        Arc::new(StringArray::from(self.table_types)),
                        Arc::new(table_columns),
                        Arc::new(table_constraints),
                    ],
                    None,
                );

                ListArray::new(
                    table_item,
                    offsets_from_counts(&self.schema_table_counts),
                    Arc::new(tables_struct),
                    None,
                )
            } else {
                ListArray::new_null(table_item, num_schemas)
            };

            let db_schemas_struct = StructArray::new(
                struct_fields(&schemas::OBJECTS_DB_SCHEMA_SCHEMA),
                vec![
                    Arc::new(StringArray::from(self.db_schema_names)) as ArrayRef,
                    Arc::new(db_schema_tables),
                ],
                None,
            );

            ListArray::new(
                db_schema_item,
                offsets_from_counts(&self.catalog_schema_counts),
                Arc::new(db_schemas_struct),
                None,
            )
        } else {
            ListArray::new_null(db_schema_item, num_catalogs)
        };

        Ok(RecordBatch::try_new(
            schemas::GET_OBJECTS_SCHEMA.clone(),
            vec![
                Arc::new(StringArray::from(self.catalog_names)),
                Arc::new(catalog_db_schemas),
            ],
        )?)
    }
}

/// Build the column-level struct array (one element per column).
fn build_columns_struct(columns: &[ColumnSchema]) -> ArrayRef {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(
            columns.iter().map(|c| &c.column_name),
        )),
        Arc::new(Int32Array::from_iter(
            columns.iter().map(|c| c.ordinal_position),
        )),
        Arc::new(StringArray::from_iter(
            columns.iter().map(|c| c.remarks.clone()),
        )),
        Arc::new(Int16Array::from_iter(
            columns.iter().map(|c| c.xdbc_data_type),
        )),
        Arc::new(StringArray::from_iter(
            columns.iter().map(|c| c.xdbc_type_name.clone()),
        )),
        Arc::new(Int32Array::from_iter(
            columns.iter().map(|c| c.xdbc_column_size),
        )),
        Arc::new(Int16Array::from_iter(
            columns.iter().map(|c| c.xdbc_decimal_digits),
        )),
        Arc::new(Int16Array::from_iter(
            columns.iter().map(|c| c.xdbc_num_prec_radix),
        )),
        Arc::new(Int16Array::from_iter(
            columns.iter().map(|c| c.xdbc_nullable),
        )),
        Arc::new(StringArray::from_iter(
            columns.iter().map(|c| c.xdbc_column_def.clone()),
        )),
        Arc::new(Int16Array::from_iter(
            columns.iter().map(|c| c.xdbc_sql_data_type),
        )),
        Arc::new(Int16Array::from_iter(
            columns.iter().map(|c| c.xdbc_datetime_sub),
        )),
        Arc::new(Int32Array::from_iter(
            columns.iter().map(|c| c.xdbc_char_octet_length),
        )),
        Arc::new(StringArray::from_iter(
            columns.iter().map(|c| c.xdbc_is_nullable.clone()),
        )),
        Arc::new(StringArray::from_iter(
            columns.iter().map(|c| c.xdbc_scope_catalog.clone()),
        )),
        Arc::new(StringArray::from_iter(
            columns.iter().map(|c| c.xdbc_scope_schema.clone()),
        )),
        Arc::new(StringArray::from_iter(
            columns.iter().map(|c| c.xdbc_scope_table.clone()),
        )),
        Arc::new(BooleanArray::from_iter(
            columns.iter().map(|c| c.xdbc_is_autoincrement),
        )),
        Arc::new(BooleanArray::from_iter(
            columns.iter().map(|c| c.xdbc_is_generatedcolumn),
        )),
    ];
    Arc::new(StructArray::new(
        struct_fields(&schemas::COLUMN_SCHEMA),
        arrays,
        None,
    ))
}

/// Build the constraint-level struct array (one element per constraint).
fn build_constraints_struct(constraints: &[TableConstraint]) -> ArrayRef {
    let mut column_name_counts = Vec::with_capacity(constraints.len());
    let mut column_names: Vec<String> = Vec::new();
    for constraint in constraints {
        column_name_counts.push(constraint.constraint_column_names.len() as i32);
        column_names.extend(constraint.constraint_column_names.iter().cloned());
    }
    let constraint_column_names = ListArray::new(
        list_item(DataType::Utf8),
        offsets_from_counts(&column_name_counts),
        Arc::new(StringArray::from(column_names)),
        None,
    );

    let mut usage_counts = Vec::with_capacity(constraints.len());
    let mut usages: Vec<&ConstraintUsage> = Vec::new();
    for constraint in constraints {
        usage_counts.push(constraint.constraint_column_usage.len() as i32);
        usages.extend(constraint.constraint_column_usage.iter());
    }
    let usage_struct = StructArray::new(
        struct_fields(&schemas::USAGE_SCHEMA),
        vec![
            Arc::new(StringArray::from_iter(
                usages.iter().map(|u| u.fk_catalog.clone()),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter(
                usages.iter().map(|u| u.fk_db_schema.clone()),
            )),
            Arc::new(StringArray::from_iter_values(
                usages.iter().map(|u| &u.fk_table),
            )),
            Arc::new(StringArray::from_iter_values(
                usages.iter().map(|u| &u.fk_column_name),
            )),
        ],
        None,
    );
    let constraint_column_usage = ListArray::new(
        list_item(schemas::USAGE_SCHEMA.clone()),
        offsets_from_counts(&usage_counts),
        Arc::new(usage_struct),
        None,
    );

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter(
            constraints.iter().map(|c| c.constraint_name.clone()),
        )),
        Arc::new(StringArray::from_iter_values(
            constraints.iter().map(|c| &c.constraint_type),
        )),
        Arc::new(constraint_column_names),
        Arc::new(constraint_column_usage),
    ];
    Arc::new(StructArray::new(
        struct_fields(&schemas::CONSTRAINT_SCHEMA),
        arrays,
        None,
    ))
}

/// A statistic value, matching one variant of the `statistic_value` union.
#[derive(Debug, Clone)]
pub enum StatisticValue {
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Binary(Vec<u8>),
}

/// Builder for the result of [get_statistics][crate::Connection::get_statistics].
///
/// Like [`GetObjectsBuilder`], the catalog → schema → statistic hierarchy is
/// built by appending in order: [`append_catalog`][GetStatisticsBuilder::append_catalog],
/// then [`append_db_schema`][GetStatisticsBuilder::append_db_schema], then
/// [`append_statistic`][GetStatisticsBuilder::append_statistic].
#[derive(Default)]
pub struct GetStatisticsBuilder {
    catalog_names: Vec<Option<String>>,
    catalog_schema_counts: Vec<i32>,
    db_schema_names: Vec<Option<String>>,
    schema_statistic_counts: Vec<i32>,
    table_names: Vec<String>,
    column_names: Vec<Option<String>>,
    statistic_keys: Vec<i16>,
    is_approximate: Vec<bool>,
    value_type_ids: Vec<i8>,
    value_offsets: Vec<i32>,
    int64_values: Int64Builder,
    uint64_values: UInt64Builder,
    float64_values: Float64Builder,
    binary_values: BinaryBuilder,
}

impl GetStatisticsBuilder {
    /// Create an empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Begin a new catalog.
    pub fn append_catalog(&mut self, name: Option<&str>) -> &mut Self {
        self.catalog_names.push(name.map(String::from));
        self.catalog_schema_counts.push(0);
        self
    }

    /// Begin a new schema within the most recently appended catalog.
    pub fn append_db_schema(&mut self, name: Option<&str>) -> &mut Self {
        self.db_schema_names.push(name.map(String::from));
        self.schema_statistic_counts.push(0);
        *self
            .catalog_schema_counts
            .last_mut()
            .expect("append_catalog must be called before append_db_schema") += 1;
        self
    }

    /// Add a statistic to the most recently appended schema.
    pub fn append_statistic(
        &mut self,
        table_name: &str,
        column_name: Option<&str>,
        key: impl Into<i16>,
        value: StatisticValue,
        is_approximate: bool,
    ) -> &mut Self {
        self.table_names.push(table_name.to_string());
        self.column_names.push(column_name.map(String::from));
        self.statistic_keys.push(key.into());
        self.is_approximate.push(is_approximate);
        match value {
            StatisticValue::Int64(v) => {
                self.value_type_ids.push(0);
                self.value_offsets.push(self.int64_values.len() as i32);
                self.int64_values.append_value(v);
            }
            StatisticValue::UInt64(v) => {
                self.value_type_ids.push(1);
                self.value_offsets.push(self.uint64_values.len() as i32);
                self.uint64_values.append_value(v);
            }
            StatisticValue::Float64(v) => {
                self.value_type_ids.push(2);
                self.value_offsets.push(self.float64_values.len() as i32);
                self.float64_values.append_value(v);
            }
            StatisticValue::Binary(v) => {
                self.value_type_ids.push(3);
                self.value_offsets.push(self.binary_values.len() as i32);
                self.binary_values.append_value(v);
            }
        }
        *self
            .schema_statistic_counts
            .last_mut()
            .expect("append_db_schema must be called before append_statistic") += 1;
        self
    }

    /// Finish building and produce a batch conforming to
    /// [`GET_STATISTICS_SCHEMA`][schemas::GET_STATISTICS_SCHEMA].
    pub fn finish(mut self) -> Result<RecordBatch> {
        let statistic_value = UnionArray::try_new(
            union_fields(&schemas::STATISTIC_VALUE_SCHEMA),
            ScalarBuffer::from(self.value_type_ids),
            Some(ScalarBuffer::from(self.value_offsets)),
            vec![
                Arc::new(self.int64_values.finish()),
                Arc::new(self.uint64_values.finish()),
                Arc::new(self.float64_values.finish()),
                Arc::new(self.binary_values.finish()),
            ],
        )?;

        let statistics_struct = StructArray::new(
            struct_fields(&schemas::STATISTICS_SCHEMA),
            vec![
                Arc::new(StringArray::from(self.table_names)) as ArrayRef,
                Arc::new(StringArray::from(self.column_names)),
                Arc::new(Int16Array::from(self.statistic_keys)),
                Arc::new(statistic_value),
                Arc::new(BooleanArray::from(self.is_approximate)),
            ],
            None,
        );
        let db_schema_statistics = ListArray::new(
            list_item(schemas::STATISTICS_SCHEMA.clone()),
            offsets_from_counts(&self.schema_statistic_counts),
            Arc::new(statistics_struct),
            None,
        );

        let db_schemas_struct = StructArray::new(
            struct_fields(&schemas::STATISTICS_DB_SCHEMA_SCHEMA),
            vec![
                Arc::new(StringArray::from(self.db_schema_names)) as ArrayRef,
                Arc::new(db_schema_statistics),
            ],
            None,
        );
        let catalog_db_schemas = ListArray::new(
            list_item(schemas::STATISTICS_DB_SCHEMA_SCHEMA.clone()),
            offsets_from_counts(&self.catalog_schema_counts),
            Arc::new(db_schemas_struct),
            None,
        );

        Ok(RecordBatch::try_new(
            schemas::GET_STATISTICS_SCHEMA.clone(),
            vec![
                Arc::new(StringArray::from(self.catalog_names)),
                Arc::new(catalog_db_schemas),
            ],
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::Statistics;

    #[test]
    fn get_info_all_variants() {
        let mut builder = GetInfoBuilder::new();
        builder
            .append_string(InfoCode::VendorName, "MyVendor")
            .append_bool(InfoCode::VendorSql, true)
            .append_int64(InfoCode::DriverAdbcVersion, 42)
            .append_int32_bitmask(InfoCode::DriverName, 1337)
            .append_string_list(InfoCode::DriverVersion, ["Hello", "World"])
            .append_int32_to_int32_list_map(
                InfoCode::DriverArrowVersion,
                [(42, vec![1, 2, 3]), (1337, vec![1, 4, 9])],
            );
        let batch = builder.finish().unwrap();
        assert_eq!(batch.schema(), *schemas::GET_INFO_SCHEMA);
        assert_eq!(batch.num_rows(), 6);
    }

    #[test]
    fn get_objects_full_depth() {
        let mut builder = GetObjectsBuilder::new(ObjectDepth::All);
        builder.append_catalog(Some("cat"));
        builder.append_db_schema(Some("schema"));
        builder.append_table("t", "table");
        builder.append_column(ColumnSchema {
            ordinal_position: Some(0),
            remarks: Some("nice".to_string()),
            ..ColumnSchema::new("col")
        });
        builder.append_constraint(TableConstraint {
            constraint_name: Some("pk".to_string()),
            constraint_type: "PRIMARY KEY".to_string(),
            constraint_column_names: vec!["col".to_string()],
            constraint_column_usage: vec![ConstraintUsage {
                fk_table: "t".to_string(),
                fk_column_name: "col".to_string(),
                ..Default::default()
            }],
        });
        let batch = builder.finish().unwrap();
        assert_eq!(batch.schema(), *schemas::GET_OBJECTS_SCHEMA);
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn get_objects_catalog_depth_nulls_schemas() {
        let mut builder = GetObjectsBuilder::new(ObjectDepth::Catalogs);
        builder.append_catalog(Some("cat"));
        let batch = builder.finish().unwrap();
        assert_eq!(batch.schema(), *schemas::GET_OBJECTS_SCHEMA);
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.column(1).null_count(), 1);
    }

    #[test]
    fn get_statistics_basic() {
        let mut builder = GetStatisticsBuilder::new();
        builder.append_catalog(Some("cat"));
        builder.append_db_schema(Some("schema"));
        builder.append_statistic(
            "t",
            Some("col"),
            Statistics::AverageByteWidth,
            StatisticValue::UInt64(42),
            false,
        );
        let batch = builder.finish().unwrap();
        assert_eq!(batch.schema(), *schemas::GET_STATISTICS_SCHEMA);
        assert_eq!(batch.num_rows(), 1);
    }
}
