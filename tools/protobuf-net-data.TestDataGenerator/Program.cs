using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;

namespace ProtoBuf.Data.TestDataGenerator
{
    internal class Program
    {
        private static Random random = new Random();

        private static Dictionary<Type, Func<object>> generators = new Dictionary<Type, Func<object>>()
        {
            { typeof(bool), () => random.Next(1) != 0 },
            { typeof(byte), () => (byte)random.Next() },
            { typeof(byte[]), () => new[] { (byte)random.Next(), (byte)random.Next() } },
            { typeof(char), () => (char)random.Next(65, 90) },
            { typeof(char[]), () => new[] { (char)random.Next(65, 90), (char)random.Next(65, 90) } },
            { typeof(DateTime), () => DateTime.UtcNow.AddTicks(random.Next()) },
            { typeof(decimal), () => Convert.ToDecimal(random.NextDouble()) },
            { typeof(double), () => random.NextDouble() },
            { typeof(float), () => Convert.ToSingle(random.NextDouble()) },
            { typeof(Guid), () => Guid.NewGuid() },
            { typeof(int), () => random.Next() },
            { typeof(long), () => Convert.ToInt64(random.Next()) },
            { typeof(short), () => Convert.ToInt16(random.Next(short.MaxValue)) },
            { typeof(string), () => new string(new[] { (char)random.Next(65, 90), (char)random.Next(65, 90) }) },
            { typeof(TimeSpan), () => TimeSpan.FromTicks(random.Next()) },
        };

        public static void Main(string[] args)
        {
            var dataSet = CreateDataSet();
            var dataReader = dataSet.CreateDataReader();

            var assembly = GetAssembly();
            var assemblyVersion = FileVersionInfo.GetVersionInfo(assembly.Location).ProductVersion;

            using (var stream = File.Create($"{assemblyVersion}.proto"))
            {
                DataSerializer.Serialize(stream, dataReader);
            }

            using (var stream = File.Create($"{assemblyVersion}.xml"))
            {
                dataSet.WriteXml(stream, XmlWriteMode.WriteSchema);
            }
        }

        private static Assembly GetAssembly()
        {
            return AppDomain.CurrentDomain.GetAssemblies()
                .First(assembly => assembly.GetName().Name == "protobuf-net-data");
        }

        private static DataSet CreateDataSet()
        {
            var dataSet = new DataSet();

            dataSet.Tables.Add(CreateDataTable(1));
            dataSet.Tables.Add(CreateDataTable(2));

            return dataSet;
        }

        private static DataTable CreateDataTable(int tableIndex)
        {
            var dataTable = new DataTable();

            for (var columnIndex = 0; columnIndex < generators.Count; columnIndex++)
            {
                dataTable.Columns.Add($"Table{tableIndex}_Column{columnIndex}", generators.Keys.ElementAt(columnIndex));
            }

            dataTable.Rows.Add(CreateDataRow());
            dataTable.Rows.Add(CreateDataRow());

            return dataTable;
        }

        private static object[] CreateDataRow()
        {
            var values = new object[generators.Values.Count];

            for (var columnIndex = 0; columnIndex < generators.Count; columnIndex++)
            {
                values[columnIndex] = generators.Values.ElementAt(columnIndex)();
            }

            return values;
        }
    }
}
