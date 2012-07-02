Protocol Buffers DataReader Extensions for .NET
================================================

A library for serializing ADO.NET DataTables and DataReaders into an efficient, portable binary format. Uses [Marc Gravell](http://marcgravell.blogspot.com/)'s Google Protocol Buffers library, [protobuf-net](http://code.google.com/p/protobuf-net/). 


### [>>> Get protobuf-net-data via NuGet](http://nuget.org/List/Packages/protobuf-net-data)



The latest protobuf-net-data is **now available in NuGet [here](http://nuget.org/List/Packages/protobuf-net-data)**, or as a zip from the downloads page.

## Usage examples

Writing a DataTable to a file:

```csharp
DataTable dt = ...;

using (Stream stream = File.OpenWrite("C:\foo.dat"))
using (IDataReader reader = dt.CreateDataReader())
{
    DataSerializer.Serialize(stream, reader);
}
```
    
Loading a DataTable from a file:

```csharp
DataTable dt = new DataTable();
    
using (Stream stream = File.OpenRead("C:\foo.dat"))
using (IDataReader reader = DataSerializer.Deserialize(stream))
{
    dt.Load(reader);
}
```
Serializing an IDataReader into a buffer... and back again:

```csharp
Stream buffer = new MemoryStream();

// Serialize SQL results to a buffer
using (var command = new SqlCommand("SELECT * FROM ..."))
using (var reader = command.ExecuteReader())
    DataSerializer.Serialize(buffer, reader);

// Read them back
buffer.Seek(0, SeekOrigin.Begin);
using (var reader = DataSerializer.Deserialize(buffer))
{
    while (reader.Read())
    {
        ...
    }
}
```

### Supported Data Types

DataSerializer supports all the types exposed by [IDataReader](http://msdn.microsoft.com/en-us/library/system.data.idatareader.aspx):

* Boolean
* Byte
* Byte[]
* Char
* Char[]
* DateTime
* Decimal
* Double
* Float
* Guid
* Int16
* Int32
* Int64
* String
* DataTable

Note that no distinction is made between null and zero-length arrays; both will be deserialized as null.

### Custom serialization options

This library supports custom serialization options for data tables/data readers.

```csharp
var options = new ProtoDataWriterOptions
{
    SerializeEmptyArraysAsNull = true,
    IncludeComputedColumns = true
};

DataSerializer.Serialize(buffer, reader, options);
```

The following options are currently supported:

* **SerializeEmptyArraysAsNull**: In versions 2.0.4.480 and earlier, zero-length arrays were serialized as null. After that, they are serialized properly as a zero-length array. Set this flag if you need to write to the old format. Default is false.
* **IncludeComputedColumns**: Computed columns are ignored by default (columns who's values are determined by an Expression rather than a stored value). Set to  true to include computed columns in serialization.

# Why does this library exist?

.NET, as a mostly-statically typed language, has a lot of really good options for serializing statically-typed objects. Protocol Buffers, MessagePack, JSON, BSON, XML, SOAP, and the BCL's own proprietary binary serialization are all great for CLR objects, where the fields can be determined at runtime.

However, for data that is tabular in nature, there aren't so many options. **Protocol Buffers DataReader Extensions for .NET** was born out of a need to serialize data:

* That is tabular - not necessarily CLR DTOs.
* Where the schema is unknown before it is deserialized - each data set can have totally different columns.
* In a way that is streamable, so entire entire data sets do not have to be buffered in memory at once.
* that can be as large as hundreds of thousands of rows/columns.
* In a reasonably performant manner.
* In a way that could potentially be read by different platforms.
* Into as small a number of bytes as possible.

DataSerializer packs data faster and smaller than the equivalent DataTable.Save/Write XML:

![DataSerializer vs DataTable benchmarks](http://julana.richarddingwall.name/protobuf-net-data-benchmark-1.png "Benchmarks serializing and deserializing the DimCustomer table from the AdventureWorksDW2008R2 database on an i7 620 MacBook Pro running Windows 7.")

# FAQ

#### Are multiple result sets supported?
Yes! Multiple data tables ([IDataReader.NextResult()](http://msdn.microsoft.com/en-us/library/system.data.idatareader.nextresult.aspx)) are now supported. For example, a DataSet containing the results of 3 SQL queries executed as a single batch.

#### Are nested DataTables supported?
Yes! Nested DataTables are supported. However note that unlike serializing an IDataReader (the goal of this library), a DataTable needs to be entirely buffered in memory and cannot stream its contents row-by-row. This library is designed for simple tabular data like time series, price curves, matrices, CSV files, and SQL query results. Complex or heirarchical data structures should be serialized with JSON or XML.

#### What exactly from the data reader gets serialized?
Only the data reader's contents are serialized - i.e., the column name, data type, and values. Metadata about unique keys, auto increment, default value, base table name, data provider, data relations etc is ignored. Any other [DataRowVersions](http://msdn.microsoft.com/en-us/library/system.data.datarowversion.aspx) will also ignored.

#### What about computed columns?
Computed columns (i.e. those with an [Expression](http://msdn.microsoft.com/en-us/library/system.data.datacolumn.expression.aspx) set) will be skipped and not written to the byte stream (as of 2.0.3.480).

#### Will protobuf-net v1 be supported?
No. Only protobuf-net v2 is supported right now, and it is unlikely any effort will be spent back-porting it to v1 (if indeed it is even possible with v1).

### What about backwards compatiblity?
This library is backwards compatible with itself (old versions can deserialize binary blobs produced from later versions and vice versa). The only change to the binary serialization format is that prior to version 2.0.4.480, empty arrays were serialized as null. This behaviour is not a breaking change, but will produce different output. The old behaviour can be restored in the current version by setting the SerializeEmptyArraysAsNull option to true.

#### How can I mock/stub out the DataSerializer class in my unit tests? All its methods are static.
You can use IDataSerializerEngine/DataSerializerEngine for testing and dependency injection - it has all the same methods as DataSerializer (new in [2.0.2.480](https://nuget.org/packages/protobuf-net-data/2.0.2.480)). Alternatively, both the lower-level classes, ProtoDataReader and ProtoDataWriter, have interfaces and can be mocked out as well.

#### Is this library supported in any other languages? E.g. for Java ResultSets?
In theory, protobuf-net-data binary streams should be able to be serialized and deserialized by any programming language with a protocol-buffers implementation. The protocol buffer structure is documented in ProtoDataWriter.cs.

This would be a great future roadmap - as far as I know there is currently no tool for cross-platform binary (and streaming) serialization of tabular data.

# Credits

Thanks to [Marc Gravell](http://marcgravell.blogspot.com/) for [protobuf-net](http://code.google.com/p/protobuf-net/), and the original [DataTableSerializer](http://code.google.com/p/protobuf-net/source/browse/trunk/DataTableSerializer) from which the current implementation of this library is based.

# License

Protocol Buffers DataReader Extensions for .NET is available under the [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0).

# Release History / Changelog

#### 2.0.5.480 - July 2 2012
* New feature ProtoDataWriterOptions to specify handling of zero-length arrays and computed columns.
* Bug fix for an issue where an exception would be thrown when serializing char column values (issue #15).

#### 2.0.4.480 - June 27 2012
* Bug fix for an issue where ProtoDataWriter incorrectly assumed all IDataReader schema tables have an 'Expression' column (issue #12).

#### 2.0.3.480 - March 16 2012
* Bug fix for an issue where computed columns were serialized (issue #11).

#### 2.0.2.480 - February 18 2012
* Extracted IDataSerializerEngine to make mocking and dependency injection easier.

#### 2.0.1.480 - January 11 2012
* Upgraded to protobuf-net 2.0.0.480.
* Fixed an issue saving floats and doubles (issue #10).

#### 2.0.1.470 - December 8 2011
* Upgraded to protobuf-net 2.0.0.470 (issue #9).
* Version number had to incremented unfortunately due to me uploading a broken package to NuGet.org (version numbers can't be reused).

#### 2.0.0.452 - November 1 2011
* Initial release.