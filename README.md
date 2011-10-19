Protocol Buffers DataReader Extensions for .NET
================================================

A library for serializing ADO.NET DataTables and DataReaders into an efficient, portable binary format. Uses [Marc Gravell](http://marcgravell.blogspot.com/)'s Google Protocol Buffers library, [protobuf-net](http://code.google.com/p/protobuf-net/). 

A **beta version** is available on the [downloads](https://github.com/rdingwall/protobuf-net-data/downloads) page (built against protobuf-net v2 beta r450).

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
Yes! Multiple result sets - i.e, IDataReader.NextResult() - are now supported. For example, a DataSet containing the results of 3 SQL queries executed as a single batch.

#### Are nested DataTables supported?
Yes! Nested DataTables are supported. However note that unlike an IDataReader, a DataTable needs to be entirely buffered in memory and cannot stream its contents row-by-row. This library is designed for simple tabular data like time series, matrices, CSV files, and SQL query results. Complex or heirarchical data structures should be serialized with JSON or XML.

#### What exactly from the data reader gets serialized?
Only the data reader's contents are serialized - i.e., the column name, data type, and values. Metadata about unique keys, auto increment, default value, base table name, data provider, data relations etc is ignored.

#### Will protobuf-net v1 be supported?
No. Only protobuf-net v2 is supported right now, and it is unlikely any effort will be spent back-porting it to v1 (if indeed it is even possible with v1).


# Credits

Thanks to [Marc Gravell](http://marcgravell.blogspot.com/) for [protobuf-net](http://code.google.com/p/protobuf-net/), and the original [DataTableSerializer](http://code.google.com/p/protobuf-net/source/browse/trunk/DataTableSerializer) from which the current implementation of this library is based.

# License

Protocol Buffers DataReader Extensions for .NET is available under the [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0).
