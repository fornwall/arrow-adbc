<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Understanding an ADBC Driver from First Principles

This document is a guided tour of what an ADBC driver *is* and how one is put
together in this repository. It assumes no prior knowledge of Arrow or ADBC. If
you can read C function signatures, you can follow along.

The goal here is the **big picture**: the entrypoint into the shared library,
the shape of the ADBC interface, and how the pieces fit. It deliberately does
*not* dig deep into any single driver's internals — once you understand the
frame, the individual drivers are much easier to read on your own.

---

## 1. The problem ADBC solves

Every database has its own client library with its own API: libpq for
PostgreSQL, the SQLite C API, a Flight SQL client, and so on. If your
application wants to support five databases, you historically wrote five
different integrations, and each one handed you data in a different in-memory
shape (rows of tagged unions, driver-specific buffers, language objects).

Two older standards, **ODBC** and **JDBC**, tackle the "many databases, one
API" problem. But they are *row-oriented*: you fetch one row at a time, cell by
cell. That is a poor fit for analytics, where you want to move millions of rows
of columnar data as fast as possible.

**ADBC (Arrow Database Connectivity)** is a third option focused on exactly that
case. It is an **API standard** — not a wire protocol — with one defining
choice:

> Every result set comes back as **Apache Arrow** data, and every set of query
> parameters is passed in as Arrow data.

Because the data format is fixed and columnar, an application writes its logic
against ADBC *once* and can then talk to any database that ships an ADBC driver,
with no per-database data conversion.

### The 30-second Arrow refresher

Apache Arrow is a **standardized columnar in-memory layout** for tabular data.
Instead of storing a table row by row, it stores each column as a contiguous
block of memory. The important consequence for ADBC is Arrow's **C Data
Interface**: two tiny C structs, `ArrowSchema` (describes the columns) and
`ArrowArray` (holds a chunk of column data), plus `ArrowArrayStream` (an
iterator that yields a sequence of `ArrowArray`s — i.e. a streamed result set).

These structs are a stable ABI that any language can produce or consume without
linking against Arrow itself. ADBC leans on them directly: a query result *is*
an `ArrowArrayStream`. That is why a driver written in C can hand results to
Python, Rust, or Go with **no copying and no serialization**.

You do not need to understand Arrow's internal byte layout to work with ADBC —
just know that `ArrowArrayStream` is "a result set" and `ArrowSchema` is "the
column types."

---

## 2. The shape of the ADBC interface

ADBC models a database session with **three objects**, arranged as a hierarchy.
Each is a small handle struct defined in
[`c/include/arrow-adbc/adbc.h`](../c/include/arrow-adbc/adbc.h):

| Object          | Represents                                        | Analogy (JDBC)   |
| --------------- | ------------------------------------------------- | ---------------- |
| `AdbcDatabase`  | Shared, connection-independent state              | `DataSource`     |
| `AdbcConnection`| A single active session against the database      | `Connection`     |
| `AdbcStatement` | One query (or prepared query) and its parameters  | `Statement`      |

You create them top-down: a database owns connections, a connection owns
statements. Each handle is intentionally tiny — just two opaque pointers:

```c
struct AdbcDatabase {
  void* private_data;             // driver-owned state; NULL until New/after Release
  struct AdbcDriver* private_driver;  // which driver backs this handle
};
```

`AdbcConnection` and `AdbcStatement` have the same two fields. All the real
state lives behind `private_data`, whose layout only the driver knows. This is
the classic "opaque handle" pattern: the caller holds a pointer it cannot look
inside, and every operation is a function that takes the handle back.

### The functions

For every object there is a family of C functions named `Adbc<Object><Verb>`.
The lifecycle verbs are consistent across all three:

- `New` — allocate and zero-initialize the handle.
- `SetOption` — configure it (e.g. a connection URI, an ingestion target table).
- `Init` — actually open/establish it, using the options set so far.
- `Release` — tear it down and free `private_data`.

So a minimal end-to-end flow looks like this (error handling elided):

```c
AdbcDatabase   db  = {0};
AdbcConnection cxn = {0};
AdbcStatement  stmt = {0};
AdbcError      error = {0};

AdbcDatabaseNew(&db, &error);
AdbcDatabaseSetOption(&db, "uri", "file:example.db", &error);
AdbcDatabaseInit(&db, &error);

AdbcConnectionNew(&cxn, &error);
AdbcConnectionInit(&cxn, &db, &error);

AdbcStatementNew(&cxn, &stmt, &error);
AdbcStatementSetSqlQuery(&stmt, "SELECT * FROM foo", &error);

struct ArrowArrayStream results;   // <-- the Arrow result set
int64_t rows_affected;
AdbcStatementExecuteQuery(&stmt, &results, &rows_affected, &error);

// ... iterate `results` (an ArrowArrayStream) to read the data ...

AdbcStatementRelease(&stmt, &error);
AdbcConnectionRelease(&cxn, &error);
AdbcDatabaseRelease(&db, &error);
```

Notice that the *only* Arrow-specific type the caller touches is
`ArrowArrayStream`. Everything else is plain C strings and integers.

### Errors: the `AdbcStatusCode` + `AdbcError` pair

Every ADBC function returns an `AdbcStatusCode` (a `uint8_t`) and takes an
optional `AdbcError*` out-parameter for a detailed message. `ADBC_STATUS_OK`
(0) means success; non-zero values classify the failure:

| Code                          | Meaning                                   |
| ----------------------------- | ----------------------------------------- |
| `ADBC_STATUS_NOT_IMPLEMENTED` | Operation/feature not supported by driver |
| `ADBC_STATUS_NOT_FOUND`       | Requested thing does not exist            |
| `ADBC_STATUS_INVALID_ARGUMENT`| Caller passed something bad               |
| `ADBC_STATUS_INVALID_STATE`   | Called out of order (e.g. no `New` yet)   |
| `ADBC_STATUS_IO`              | Underlying I/O / network failure          |
| ...                           | (see `adbc.h` for the full list)          |

When a call fails, the driver fills in `error.message` and sets an
`error.release` callback; the caller reads the message, then invokes
`error.release` to free it. This "the object carries its own destructor"
pattern shows up throughout ADBC (and Arrow) so that memory allocated on one
side of a library boundary is always freed by the same side.

---

## 3. The shared library entrypoint

Here is the crux of how ADBC stays vendor-neutral. A driver is compiled into a
shared library (`.so` / `.dll` / `.dylib`) that exports **exactly one symbol
that matters**: an initialization function matching this signature (bottom of
`adbc.h`):

```c
typedef AdbcStatusCode (*AdbcDriverInitFunc)(int version, void* driver,
                                             struct AdbcError* error);
```

By convention the exported symbol is called `AdbcDriverInit` (drivers may also
export a uniquely-named alias like `AdbcDriverSqliteInit` so several drivers can
be statically linked into one binary without symbol clashes). You can see this
at the very bottom of the SQLite driver in
[`c/driver/sqlite/sqlite.cc`](../c/driver/sqlite/sqlite.cc):

```c
AdbcStatusCode AdbcDriverInit(int version, void* raw_driver, AdbcError* error) {
  return adbc::sqlite::SqliteDriver::Init(version, raw_driver, error);
}
```

### What the entrypoint does

The `driver` parameter points to an **`AdbcDriver` struct** — a big **table of
function pointers** (a vtable). The entrypoint's whole job is to fill that table
in with the driver's implementations:

```c
struct AdbcDriver {
  void* private_data;
  void* private_manager;
  AdbcStatusCode (*release)(struct AdbcDriver*, struct AdbcError*);

  AdbcStatusCode (*DatabaseNew)(struct AdbcDatabase*, struct AdbcError*);
  AdbcStatusCode (*DatabaseInit)(struct AdbcDatabase*, struct AdbcError*);
  AdbcStatusCode (*DatabaseSetOption)(struct AdbcDatabase*, const char*, const char*,
                                      struct AdbcError*);
  // ... one function pointer for every AdbcConnection* and AdbcStatement* operation ...
  AdbcStatusCode (*StatementExecuteQuery)(struct AdbcStatement*, struct ArrowArrayStream*,
                                          int64_t*, struct AdbcError*);
  // ...
};
```

So the flow at load time is:

1. The caller allocates a zeroed `AdbcDriver` struct.
2. The caller calls `AdbcDriverInit(version, &driver, &error)`.
3. The driver populates every function pointer with its own implementation, and
   sets `driver.private_data` to any global state it needs.

From then on, the caller invokes database operations *through the vtable*, e.g.
`driver.StatementExecuteQuery(&stmt, ...)`.

### The `version` parameter and forward compatibility

The `version` argument (`ADBC_VERSION_1_0_0`, `ADBC_VERSION_1_1_0`, ...) is how
ADBC evolves without breaking old binaries. The `AdbcDriver` struct only ever
*grows* — new function pointers are appended at the end. The version tells the
driver which size of struct the caller allocated, so it knows how many fields it
is allowed to write.

- A new driver loaded by an old caller: the driver is asked for `1_0_0`, fills
  only the original fields, leaves the newer features unused.
- An old driver loaded by a new caller: asked for `1_1_0`, it returns
  `ADBC_STATUS_NOT_IMPLEMENTED`; the caller retries with `1_0_0`.

This negotiation means a single ADBC application can load drivers built against
different revisions of the standard. It is the same trick the Arrow C Data
Interface uses, and it is the reason the struct layout in `adbc.h` must never be
reordered — only appended to.

---

## 4. Two ways to use a driver

There are two distinct ways the vtable above gets in front of an application.

### (a) Link the driver directly

If you build against a driver's shared library, you call its `AdbcDriverInit`
yourself, get the vtable, and go. Simple, but your app is now tied to that one
driver at build time.

### (b) The driver manager (the JDBC/ODBC-style path)

The **driver manager** ([`c/driver_manager/`](../c/driver_manager/)) is itself a
library that *implements the same public ADBC C API* — `AdbcDatabaseNew`,
`AdbcStatementExecuteQuery`, and friends — but instead of doing database work it:

1. Reads a driver name / path from an option (e.g. `"driver": "adbc_driver_sqlite"`).
2. `dlopen`s that shared library and looks up its `AdbcDriverInit` symbol
   (default entrypoint name is `"AdbcDriverInit"`).
3. Calls it to obtain the vtable.
4. **Forwards** every subsequent public call to the corresponding vtable entry.

You can see the forwarding in
[`c/driver_manager/adbc_driver_manager_api.cc`](../c/driver_manager/adbc_driver_manager_api.cc):

```c
AdbcStatusCode AdbcStatementExecuteQuery(struct AdbcStatement* statement,
                                         struct ArrowArrayStream* out,
                                         int64_t* rows_affected,
                                         struct AdbcError* error) {
  // statement->private_driver is the vtable loaded from the shared library.
  return statement->private_driver->StatementExecuteQuery(statement, out,
                                                          rows_affected, error);
}
```

This is why every handle carries a `private_driver` pointer: it remembers which
loaded driver a given database/connection/statement belongs to, so the manager
knows which vtable to dispatch into. With the driver manager, choosing a
database becomes a runtime configuration string rather than a build-time
decision — exactly the JDBC/ODBC experience, but Arrow-native.

The higher-level language bindings (Python, Go, R, ...) sit on top of the driver
manager, which is how a single Python program can talk to PostgreSQL, SQLite, or
Flight SQL just by changing the driver name.

---

## 5. How drivers are implemented in this repository

You now understand the *contract*: export `AdbcDriverInit`, fill a vtable, back
each function pointer with an implementation. If every driver wrote that
boilerplate by hand — argument checking, option storage, error formatting,
lifecycle state machines — it would be repetitive and error-prone.

So the C/C++ drivers share a **driver framework** in
[`c/driver/framework/`](../c/driver/framework/). It provides C++ base classes
(using the [CRTP][crtp] pattern) that already implement the whole vtable and the
tedious plumbing. A concrete driver just subclasses them and overrides the
handful of methods that are actually database-specific.

The base classes mirror the three objects:

| Framework base class (`c/driver/framework/`) | You override to...                       |
| -------------------------------------------- | ---------------------------------------- |
| `Database` (`database.h`)                    | open/close the underlying DB handle      |
| `Connection` (`connection.h`)                | manage sessions, metadata, transactions  |
| `Statement` (`statement.h`)                  | run SQL and produce an `ArrowArrayStream` |
| `Driver` (`base_driver.h`)                   | tie them together; provides `Init()`     |

The framework's `Driver::Init()` is what actually populates the `AdbcDriver`
vtable — pointing each entry at a generic C shim that forwards into your C++
subclass. That is the single point where "a C++ class hierarchy" becomes "a C
struct of function pointers." A concrete driver's `AdbcDriverInit` is then just
a one-liner that calls the framework's `Init`, as shown in §3.

The concrete drivers that live in [`c/driver/`](../c/driver/) are:

| Directory                | Driver                                          |
| ------------------------ | ----------------------------------------------- |
| `c/driver/sqlite/`       | SQLite — the smallest, best starting point      |
| `c/driver/postgresql/`   | PostgreSQL (via libpq)                           |
| `c/driver/flightsql/`    | Arrow Flight SQL                                 |
| `c/driver/framework/`    | Shared base classes (not a driver itself)       |
| `c/driver/common/`       | Shared helpers                                   |

> **Where a query becomes Arrow.** The database-specific magic — turning
> SQLite/Postgres rows into Arrow columns — lives inside each driver's statement
> code (e.g. `c/driver/sqlite/statement_reader.c`). That is the deep-internals
> layer this guide intentionally stops short of; it is where you would go next
> once the framing above is clear.

There are also full implementations of the same standard in **Go**
([`go/adbc/`](../go/adbc/)), **Java** ([`java/`](../java/)), and **Rust**
([`rust/`](../rust/)). They implement the identical concepts — the same three
objects, the same vtable idea expressed idiomatically per language — so what you
learned above transfers directly.

[crtp]: https://en.wikipedia.org/wiki/Curiously_recurring_template_pattern

---

## 6. Recap / mental model

- **ADBC is an API standard**: talk to any database through one C API, and get
  results back as **Arrow** columnar data — no per-database conversion.
- The API is **three opaque handle objects** — `AdbcDatabase` →
  `AdbcConnection` → `AdbcStatement` — each driven by `New` / `SetOption` /
  `Init` / `Release` functions, all returning an `AdbcStatusCode`.
- A driver is a **shared library exporting `AdbcDriverInit`**, whose only job is
  to fill an **`AdbcDriver` vtable** of function pointers. A `version` argument
  keeps old and new binaries compatible by only ever appending to that struct.
- You either **link a driver directly** or use the **driver manager**, which
  `dlopen`s a driver by name and forwards the public API into its vtable —
  giving the JDBC/ODBC "pick your database at runtime" experience.
- In this repo, C/C++ drivers are built on a shared **framework** of CRTP base
  classes; a concrete driver overrides only its database-specific behavior. The
  same standard is also implemented in Go, Java, and Rust.

### Where to look next

| To understand...            | Read...                                          |
| --------------------------- | ------------------------------------------------ |
| The exact API contract      | [`c/include/arrow-adbc/adbc.h`](../c/include/arrow-adbc/adbc.h) (heavily commented) |
| A small, complete driver    | [`c/driver/sqlite/sqlite.cc`](../c/driver/sqlite/sqlite.cc) |
| The shared plumbing         | [`c/driver/framework/`](../c/driver/framework/)  |
| Runtime driver loading      | [`c/driver_manager/`](../c/driver_manager/)      |
| The rendered user docs      | <https://arrow.apache.org/adbc>                  |
