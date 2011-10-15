// Copyright 2011 Richard Dingwall - http://richarddingwall.name
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Data;
using System.IO;
using System.Linq;
using NUnit.Framework;
using SharpTestsEx;

namespace ProtoBuf.Data.Tests
{
    public class DataSerializerTests
    {
        [TestFixture]
        public class When_serializing_a_data_table_to_a_buffer_and_back
        {
            DataTable originalTable;
            DataTable deserializedTable;

            [TestFixtureSetUp]
            public void TestFixtureSetUp()
            {
                originalTable = TestData.SmallDataTable();

                deserializedTable = new DataTable();

                using (var stream = new MemoryStream())
                using (var originalReader = originalTable.CreateDataReader())
                {
                    DataSerializer.Serialize(stream, originalReader);

                    stream.Seek(0, SeekOrigin.Begin);

                    using (var reader = DataSerializer.Deserialize(stream))
                        deserializedTable.Load(reader);
                }
            }

            [Test]
            public void Should_produce_the_same_number_of_columns()
            {
                deserializedTable.Columns.Count.Should().Be(originalTable.Columns.Count);
            }

            [Test]
            public void The_columns_should_all_have_the_same_names()
            {
                TestHelper.AssertColumnNamesEqual(originalTable, deserializedTable);
            }

            [Test]
            public void The_columns_should_all_have_the_same_types()
            {
                TestHelper.AssertColumnTypesEqual(originalTable, deserializedTable);
            }

            [Test]
            public void Should_produce_the_same_number_of_rows()
            {
                deserializedTable.Rows.Count.Should().Be(originalTable.Rows.Count);
            }

            [Test]
            public void Should_serialize_row_values_correctly()
            {
                TestHelper.AssertRowValuesEqual(originalTable, deserializedTable);
            }
        }

        [TestFixture]
        public class When_serializing_and_deserializing
        {
            private const string TestFile = "BackwardsCompatbilityTest.bin";

            [Test]
            public void Should_retain_binary_compatibility_when_reading()
            {
                using (var expected = CreateTablesForBackwardsCompatibilityTest())
                using (var actual = new DataSet())
                {
                    using (var stream = File.OpenRead(TestFile))
                    using (var reader = DataSerializer.Deserialize(stream))
                        actual.Load(reader, LoadOption.OverwriteChanges, "A", "B", "C", "D");

                    for (var i = 0; i < expected.Tables.Count; i++)
                        TestHelper.AssertRowValuesEqual(expected.Tables[i], actual.Tables[i]);
                }
            }

            static DataSet CreateTablesForBackwardsCompatibilityTest()
            {
                var tableA = new DataTable();
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
                tableA.Rows.Add(new DateTime(2008, 01, 11, 11, 4, 1, 491), null, "Foo", Guid.Empty, "", null, new char[0]);

                var tableB = new DataTable();

                var tableC = new DataTable();
                tableC.Columns.Add("Name", typeof (string));

                var tableD = new DataTable();
                tableD.Columns.Add("Value", typeof(int));
                tableD.Rows.Add(1);
                tableD.Rows.Add(2);
                tableD.Rows.Add(3);

                var dataSet = new DataSet();
                dataSet.Tables.AddRange(new[] { tableA, tableB, tableC, tableD });
                return dataSet;
            }

            [Test]
            public void Should_retain_binary_compatibility_when_writing()
            {
                using (var dataSet = CreateTablesForBackwardsCompatibilityTest())
                using (var stream = new MemoryStream())
                {
                    using (var reader = dataSet.CreateDataReader())
                        DataSerializer.Serialize(stream, reader);

                    var expected = File.ReadAllBytes(TestFile);

                    stream.Seek(0, SeekOrigin.Begin);

                    stream.GetBuffer().Take(expected.Length).Should().Have.SameSequenceAs(expected);
                }
            }
        }

        [TestFixture]
        public class When_serializing_an_unsupported_type
        {
            DataTable originalTable;

            class Foo {}

            [TestFixtureSetUp]
            public void TestFixtureSetUp()
            {
                originalTable = new DataTable();
                originalTable.Columns.Add("Foo", typeof(Foo));
                originalTable.Rows.Add(new Foo());
            }

            [Test, ExpectedException(typeof(UnsupportedColumnTypeException))]
            public void Should_throw_an_exception()
            {
                using (var originalReader = originalTable.CreateDataReader())
                {
                    DataSerializer.Serialize(Stream.Null, originalReader);
                }
            }
        }

        [TestFixture]
        public class When_serializing_a_data_table_with_no_rows
        {
            DataTable originalTable;
            DataTable deserializedTable;

            [TestFixtureSetUp]
            public void TestFixtureSetUp()
            {
                originalTable = new DataTable();
                originalTable.Columns.Add("ColumnA", typeof (int));

                deserializedTable = new DataTable();

                using (var stream = new MemoryStream())
                using (var originalReader = originalTable.CreateDataReader())
                {
                    DataSerializer.Serialize(stream, originalReader);

                    stream.Seek(0, SeekOrigin.Begin);

                    using (var reader = DataSerializer.Deserialize(stream))
                        deserializedTable.Load(reader);
                }
            }

            [Test]
            public void Should_produce_the_same_number_of_columns()
            {
                deserializedTable.Columns.Count.Should().Be(originalTable.Columns.Count);
            }

            [Test]
            public void The_columns_should_all_have_the_same_names()
            {
                TestHelper.AssertColumnNamesEqual(originalTable, deserializedTable);
            }

            [Test]
            public void The_columns_should_all_have_the_same_types()
            {
                TestHelper.AssertColumnTypesEqual(originalTable, deserializedTable);
            }

            [Test]
            public void Should_produce_the_same_number_of_rows()
            {
                deserializedTable.Rows.Count.Should().Be(originalTable.Rows.Count);
            }
        }

        [TestFixture]
        public class When_serializing_a_data_table_with_no_columns_or_rows
        {
            DataTable originalTable;
            DataTable deserializedTable;

            [TestFixtureSetUp]
            public void TestFixtureSetUp()
            {
                originalTable = new DataTable();

                deserializedTable = new DataTable();

                using (var stream = new MemoryStream())
                using (var originalReader = originalTable.CreateDataReader())
                {
                    DataSerializer.Serialize(stream, originalReader);

                    stream.Seek(0, SeekOrigin.Begin);

                    using (var reader = DataSerializer.Deserialize(stream))
                        deserializedTable.Load(reader);
                }
            }

            [Test]
            public void Should_produce_the_same_number_of_columns()
            {
                deserializedTable.Columns.Count.Should().Be(originalTable.Columns.Count);
            }

            [Test]
            public void The_columns_should_all_have_the_same_names()
            {
                TestHelper.AssertColumnNamesEqual(originalTable, deserializedTable);
            }

            [Test]
            public void The_columns_should_all_have_the_same_types()
            {
                TestHelper.AssertColumnTypesEqual(originalTable, deserializedTable);
            }

            [Test]
            public void Should_produce_the_same_number_of_rows()
            {
                deserializedTable.Rows.Count.Should().Be(originalTable.Rows.Count);
            }
        }
    }
}