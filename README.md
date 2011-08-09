Protocol Buffers DataReader Extensions for .NET
================================================

A library for serializing ADO.NET DataTables and IDataReaders into a portable binary format.

Uses Marc Gravell's Google Protocol Buffers library, [protobuf-net](http://code.google.com/p/protobuf-net/).

### Examples

Write a DataTable to a file:

    DataTable dt = ...;
    
    using (Stream stream = File.OpenRead("C:\foo.dat"))
    using (IDataReader reader = dt.CreateDataReader())
    {
        DataSerializer.Serialize(stream, reader);
    }
    
Load DataTable from a file:

    DataTable dt = new DataTable();
    
    using (Stream stream = File.OpenRead("C:\foo.dat"))
    using (IDataReader reader = DataSerializer.Deserialize(stream))
    {
        dt.Load(reader);
    }
    
Serialize an IDataReader into a buffer... and back again:

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

### Known limitations/bugs

* Doesn't support nested data readers
* Doesn't support BLOB types (char[], byte[])

