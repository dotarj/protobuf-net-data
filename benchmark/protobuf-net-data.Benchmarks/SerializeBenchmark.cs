// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Data;
using System.IO;
using BenchmarkDotNet.Attributes;

namespace ProtoBuf.Data.Benchmarks
{
    [MemoryDiagnoser]
    public class SerializeBenchmark
    {
        private static readonly DataTable SchemaTable;

        static SerializeBenchmark()
        {
            SchemaTable = new DataTable();

            SchemaTable.Columns.Add("ColumnName", typeof(string));
            SchemaTable.Columns.Add("DataType", typeof(Type));

            SchemaTable.Rows.Add("foo", typeof(int));
            SchemaTable.Rows.Add("bar", typeof(Guid));
            SchemaTable.Rows.Add("baz", typeof(float));
        }

        [Benchmark]
        public void Run()
        {
            var dataReader = new DataReaderMock(SchemaTable);

            using (var memoryStream = new MemoryStream())
            {
                DataSerializer.Serialize(memoryStream, dataReader);
            }
        }
    }
}
