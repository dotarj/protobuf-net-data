// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Data;
using System.IO;
using BenchmarkDotNet.Attributes;

namespace ProtoBuf.Data.Benchmarks
{
    [MemoryDiagnoser]
    public class DeserializeBenchmark
    {
        private static readonly byte[] SerializedDataReader;

        static DeserializeBenchmark()
        {
            var schemaTable = new DataTable();

            schemaTable.Columns.Add("ColumnName", typeof(string));
            schemaTable.Columns.Add("DataType", typeof(Type));

            schemaTable.Rows.Add("foo", typeof(int));
            schemaTable.Rows.Add("bar", typeof(Guid));
            schemaTable.Rows.Add("baz", typeof(float));

            var dataReader = new DataReaderMock(1000, schemaTable);

            using (var memoryStream = new MemoryStream())
            {
                DataSerializer.Serialize(memoryStream, dataReader);

                SerializedDataReader = memoryStream.ToArray();
            }
        }

        [Benchmark]
        public void Run()
        {
            using (var memoryStream = new MemoryStream(SerializedDataReader))
            {
                var reader = DataSerializer.Deserialize(memoryStream);

                do
                {
                    reader.Read();

                    reader.GetInt32(0);
                    reader.GetGuid(1);
                    reader.GetFloat(2);
                }
                while (reader.NextResult());
            }
        }
    }
}
