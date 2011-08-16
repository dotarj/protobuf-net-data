using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;

namespace ProtoBuf.Data.Benchmark
{
    class Benchmark
    {
        long writeXmlSize;
        long protoSize;

        TimeSpan writeXmlDuration;
        TimeSpan readXmlDuration;
        TimeSpan protoSerializeDuration;
        TimeSpan protoDeserializeDuration;

        private readonly string connectionString;

        public Benchmark(string connectionString)
        {
            if (connectionString == null) throw new ArgumentNullException("connectionString");
            this.connectionString = connectionString;
        }

        public void Run()
        {
            using (var dataTable = GetData())
            {
                Console.WriteLine("Data set: {0} cols x {1} rows", dataTable.Columns.Count, dataTable.Rows.Count);

                using (var stream = new MemoryStream())
                {
                    RunWriteXmlBenchmark(stream, dataTable);
                    RunReadXmlBenchmark(stream);
                }

                using (var stream = new MemoryStream())
                {
                    RunProtoSerializeBenchmark(stream, dataTable);
                    RunProtoDeserializeBenchmark(stream);
                }
            }

            PrintResults();
        }

        private void PrintResults()
        {
            Console.WriteLine("                Packed Size    Serialize     Deserialize");
            Console.WriteLine(@"DataTable       {0,-7}       {1,-11:#.##ms}   {2:#.##ms}", writeXmlSize, writeXmlDuration.TotalMilliseconds, readXmlDuration.TotalMilliseconds);
            Console.WriteLine("DataTable       {0,-7:0x}        {1,-11:0x}   {2:0x}", 1, 1, 1);
            Console.WriteLine(@"DataSerializer  {0,-7}        {1,-11:#.##ms}   {2:#.##ms}", protoSize, protoSerializeDuration.TotalMilliseconds, protoDeserializeDuration.TotalMilliseconds);
            Console.WriteLine("DataSerializer  {0,-7:0.00x}        {1,-11:0.00x}   {2:0.00x}", (float)protoSize / writeXmlSize, 
                (float)protoSerializeDuration.Ticks / writeXmlDuration.Ticks, (float)protoDeserializeDuration.Ticks / readXmlDuration.Ticks);
        }

        public void RunWriteXmlBenchmark(Stream stream, DataTable dataTable)
        {
            Console.WriteLine("Running WriteXml() benchmark...");

            dataTable.TableName = "a";

            var sw = Stopwatch.StartNew();
            dataTable.WriteXml(stream, XmlWriteMode.WriteSchema);
            sw.Stop();

            writeXmlSize = stream.Position;
            writeXmlDuration = sw.Elapsed;

        }

        public void RunReadXmlBenchmark(Stream stream)
        {
            Console.WriteLine("Running ReadXml() benchmark...");

            stream.Seek(0, SeekOrigin.Begin);
            var sw = Stopwatch.StartNew();
            new DataTable().ReadXml(stream);
            sw.Stop();

            readXmlDuration = sw.Elapsed;
        }

        private void RunProtoDeserializeBenchmark(Stream stream)
        {
            Console.WriteLine("Running Deserialize() benchmark...");
            stream.Seek(0, SeekOrigin.Begin);

            stream.Seek(0, SeekOrigin.Begin);
            var sw = Stopwatch.StartNew();
            using (var reader = DataSerializer.Deserialize(stream))
            {
                new DataTable().Load(reader);
                sw.Stop();
            }

            protoDeserializeDuration = sw.Elapsed;
        }

        private void RunProtoSerializeBenchmark(Stream stream, DataTable dataTable)
        {
            Console.WriteLine("Running Serialize() benchmark...");

            using (var reader = dataTable.CreateDataReader())
            {
                var sw = Stopwatch.StartNew();
                DataSerializer.Serialize(stream, reader);
                sw.Stop();

                protoSize = stream.Position;
                protoSerializeDuration = sw.Elapsed;
            }
        }

        private DataTable GetData()
        {
            var dataTable = new DataTable();
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                using (var transaction = connection.BeginTransaction())
                using (var command = connection.CreateCommand())
                {
                    command.Transaction = transaction;

                    command.CommandText = "SELECT * FROM DimCustomer";

                    using (var dataReader = command.ExecuteReader())
                        dataTable.Load(dataReader);
                }
            }
            return dataTable;
        }
    }
}
