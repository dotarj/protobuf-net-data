// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    // For coverage, these tests should exercise all features of this library.
    public class BackwardsCompatibilityTests
    {
        /// <summary>
        /// Version with zero-length arrays serialized as null.
        /// </summary>
        private const string PreviousVersionTestFile = "OldBackwardsCompatbilityTest.bin";

        private const string TestFile = "BackwardsCompatbilityTest.bin";

        [Fact]
        public void ShouldBeBackwardsCompatible()
        {
            var inputs = Directory.GetFiles("./BackwardsCompatibility", "*.proto");

            foreach (var input in inputs)
            {
                var pathWithoutExtension = input.Substring(0, input.Length - 6);

                var xmlDataSet = this.LoadXmlDataSet(pathWithoutExtension + ".xml", pathWithoutExtension + ".xsd");
                var protoDataSet = this.LoadProtoDataSet(input);

                this.ValidateTables(xmlDataSet, protoDataSet);
            }
        }

        private static DataSet CreateTablesForBackwardsCompatibilityTest()
        {
            var tableA = new DataTable("A");
            tableA.Columns.Add("Birthday", typeof(DateTime));
            tableA.Columns.Add("Age", typeof(int));
            tableA.Columns.Add("Name", typeof(string));
            tableA.Columns.Add("ID", typeof(Guid));
            tableA.Columns.Add("LastName", typeof(string));
            tableA.Columns.Add("BlobData", typeof(byte[]));
            tableA.Columns.Add("ClobData", typeof(char[]));
            tableA.Rows.Add(new DateTime(2011, 04, 05, 12, 16, 41, 300), 42, "Foo", Guid.Parse("6891816b-a4b9-4749-a9f5-9f6deb377a65"), "sdfsdf", new byte[] { 1, 2, 3, 4 }, new[] { 'a' });
            tableA.Rows.Add(new DateTime(1920, 04, 03, 12, 48, 31, 210), null, "Bar", Guid.Parse("28545f31-ca0c-40c1-bae0-9b79ca84091b"), "o2389uf", new byte[0], new[] { 'a', 'b', 'c' });
            tableA.Rows.Add(null, null, null, null, null, null, null);
            tableA.Rows.Add(new DateTime(2008, 01, 11, 11, 4, 1, 491), null, "Foo", Guid.Empty, string.Empty, null, new char[0]);

            var tableB = new DataTable("B");
            tableB.Columns.Add("Name", typeof(string));

            var tableC = new DataTable("C");
            tableC.Columns.Add("Value", typeof(int));
            tableC.Rows.Add(1);
            tableC.Rows.Add(2);
            tableC.Rows.Add(3);

            var tableD = new DataTable("D");
            tableD.Columns.Add("ID", typeof(int));
            tableD.Rows.Add(42);
            tableD.Rows.Add(99);

            var dataSet = new DataSet();
            dataSet.Tables.AddRange(new[] { tableA, tableB, tableC, tableD });
            return dataSet;
        }

        private DataSet LoadXmlDataSet(string path, string schemaPath)
        {
            var xmlDataSet = new DataSet();

            xmlDataSet.ReadXmlSchema(schemaPath);
            xmlDataSet.ReadXml(path);

            return xmlDataSet;
        }

        private DataSet LoadProtoDataSet(string path)
        {
            var protoDataSet = new DataSet();

            using (var inputFile = File.OpenRead(path))
            {
                var inputDataReader = DataSerializer.Deserialize(inputFile);

                protoDataSet.Load(inputDataReader, LoadOption.OverwriteChanges, "Table1", "Table2");
            }

            return protoDataSet;
        }

        private void ValidateTables(DataSet xmlDataSet, DataSet protoDataSet)
        {
            Assert.Equal(xmlDataSet.Tables.Count, protoDataSet.Tables.Count);

            for (var tableIndex = 0; tableIndex < xmlDataSet.Tables.Count; tableIndex++)
            {
                var xmlDataTable = xmlDataSet.Tables[tableIndex];
                var protoDataTable = protoDataSet.Tables[tableIndex];

                this.ValidateColumns(xmlDataTable, protoDataTable);
                this.ValidateRows(xmlDataTable, protoDataTable);
            }
        }

        private void ValidateColumns(DataTable xmlDataTable, DataTable protoDataTable)
        {
            Assert.Equal(xmlDataTable.Columns.Count, protoDataTable.Columns.Count);

            for (var columnIndex = 0; columnIndex < xmlDataTable.Columns.Count; columnIndex++)
            {
                var xmlDataColumn = xmlDataTable.Columns[columnIndex];
                var protoDataColumn = protoDataTable.Columns[columnIndex];

                Assert.Equal(xmlDataColumn.ColumnName, protoDataColumn.ColumnName);
                Assert.Equal(xmlDataColumn.DataType, protoDataColumn.DataType);
            }
        }

        private void ValidateRows(DataTable xmlDataTable, DataTable protoDataTable)
        {
            Assert.Equal(xmlDataTable.Rows.Count, protoDataTable.Rows.Count);

            for (var rowIndex = 0; rowIndex < xmlDataTable.Rows.Count; rowIndex++)
            {
                var xmlDataRow = xmlDataTable.Rows[rowIndex];
                var protoDataRow = protoDataTable.Rows[rowIndex];

                Assert.Equal(xmlDataRow.ItemArray, protoDataRow.ItemArray);
            }
        }

        public class When_reading
        {
            [Fact]
            public void Should_retain_binary_compatibility_when_reading()
            {
                using (DataSet expected = CreateTablesForBackwardsCompatibilityTest())
                using (DataSet actual = new DataSet())
                {
                    var assembly = Assembly.GetExecutingAssembly();

                    using (var stream = assembly.GetManifestResourceStream($"{assembly.GetName().Name}.{TestFile}"))
                    using (IDataReader reader = DataSerializer.Deserialize(stream))
                    {
                        actual.Load(reader, LoadOption.PreserveChanges, "A", "B", "C", "D");
                    }

                    Assert.False(actual.HasErrors);

                    TestHelper.AssertContentsEqual(expected, actual);
                }
            }
        }

        public class When_writing
        {
            [Fact]
            public void Should_retain_binary_compatibility_when_writing()
            {
                using (DataSet dataSet = CreateTablesForBackwardsCompatibilityTest())
                using (var stream = new MemoryStream())
                {
                    using (DataTableReader reader = dataSet.CreateDataReader())
                    {
                        DataSerializer.Serialize(stream, reader);
                    }

                    byte[] expected = this.GetFile(TestFile);

                    stream.Seek(0, SeekOrigin.Begin);

                    Assert.Equal(expected, stream.GetBuffer().Take(expected.Length));
                }
            }

            [Fact]
            public void Should_retain_binary_compatibility_with_previous_versions_when_writing()
            {
                using (DataSet dataSet = CreateTablesForBackwardsCompatibilityTest())
                using (var stream = new MemoryStream())
                {
                    using (DataTableReader reader = dataSet.CreateDataReader())
                    {
                        var options = new ProtoDataWriterOptions { SerializeEmptyArraysAsNull = true };
                        DataSerializer.Serialize(stream, reader, options);
                    }

                    byte[] expected = this.GetFile(PreviousVersionTestFile);

                    stream.Seek(0, SeekOrigin.Begin);

                    Assert.Equal(expected, stream.GetBuffer().Take(expected.Length));
                }
            }

            // [Ignore("Only when our binary format changes (and we don't care about breaking old versions).")]
            [Fact]
            private void RegenerateTestFile()
            {
                using (DataSet dataSet = CreateTablesForBackwardsCompatibilityTest())
                using (FileStream stream = new FileStream(Path.Combine(@"..\..\", TestFile), FileMode.Create))
                {
                    using (DataTableReader reader = dataSet.CreateDataReader())
                    {
                        DataSerializer.Serialize(stream, reader);
                    }
                }
            }

            private byte[] GetFile(string fileName)
            {
                var assembly = Assembly.GetExecutingAssembly();

                using (var stream = assembly.GetManifestResourceStream($"{assembly.GetName().Name}.{fileName}"))
                {
                    var bytes = new List<byte>();

                    int b;
                    while ((b = stream.ReadByte()) != -1)
                    {
                        bytes.Add((byte)b);
                    }

                    return bytes.ToArray();
                }
            }
        }
    }
}