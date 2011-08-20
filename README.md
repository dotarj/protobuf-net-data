Protocol Buffers DataReader Extensions for .NET
================================================

A library for serializing ADO.NET DataTables and DataReaders into a portable binary format. Uses Marc Gravell's Google Protocol Buffers library, [protobuf-net](http://code.google.com/p/protobuf-net/).

### Examples

Writing a DataTable to a file:

    DataTable dt = ...;
    
    using (Stream stream = File.OpenWrite("C:\foo.dat"))
    using (IDataReader reader = dt.CreateDataReader())
    {
        DataSerializer.Serialize(stream, reader);
    }
    
Loading a DataTable from a file:

    DataTable dt = new DataTable();
    
    using (Stream stream = File.OpenRead("C:\foo.dat"))
    using (IDataReader reader = DataSerializer.Deserialize(stream))
    {
        dt.Load(reader);
    }
    
Serializing an IDataReader into a buffer... and back again:

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

### Supported Data Types

DataSerializer supports all the primitive types exposed by [IDataReader](http://msdn.microsoft.com/en-us/library/system.data.idatareader.aspx):

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

Note that no distinction is made between null and zero-length arrays; both will be deserialized as null.

#### Limitations

* Doesn't (yet) support nested data readers - aka [IDataReader.GetData()](http://msdn.microsoft.com/en-us/library/system.data.idatarecord.getdata.aspx)
* Doesn't (yet) support multiple DataTables in single DataReader - aka [IDataReader.NextResult()](http://msdn.microsoft.com/en-us/library/system.data.idatareader.nextresult.aspx).

### Benchmarks

DataSerializer packs data faster and smaller than DataTable.Save/Write XML. 

![DataSerializer vs DataTable benchmarks](http://julana.richarddingwall.name/protobuf-net-data-benchmark.png "Benchmarks serializing and deserializing the DimCustomer table from the AdventureWorksDW2008R2 database on an i7 620 MacBook Pro running Windows 7.")

### Roadmap

There is still some work to do before a 1.0 release, check out the [GitHub issues](http://github.com/rdingwall/protobuf-net-data/issues) for todo list and roadmap.

### License

Protocol Buffers DataReader Extensions for .NET is available under the [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0).