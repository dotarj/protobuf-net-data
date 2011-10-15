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
using System.Data.SqlClient;
using System.IO;
using NUnit.Framework;
using SharpTestsEx;

namespace ProtoBuf.Data.Tests
{
    public class MultipleDataTableResults
    {
        [TestFixture]
        public class When_serializing_a_datareader_with_multiple_result_sets
        {
            private DataSet dataSet;
            private DataSet deserializedDataSet;
            private Stream stream;

            [TestFixtureSetUp]
            public void TestFixtureSetUp()
            {
                var connectionString = new SqlConnectionStringBuilder
                {
                    DataSource = @".\SQLEXPRESS",
                    InitialCatalog = "AdventureWorksDW2008R2",
                    IntegratedSecurity = true
                };

                dataSet = new DataSet();
                using (var connection = new SqlConnection(connectionString.ConnectionString))
                {
                    connection.Open();
                    using (var transaction = connection.BeginTransaction())
                    using (var command = connection.CreateCommand())
                    {
                        command.Transaction = transaction;

                        command.CommandText = "SELECT TOP 25 * FROM DimCustomer; SELECT TOP 42 * FROM DimProduct;";

                        using (var dataReader = command.ExecuteReader())
                            dataSet.Load(dataReader, LoadOption.OverwriteChanges, "DimCustomer", "DimProduct");
                    }
                }

                deserializedDataSet = new DataSet();

                stream = new MemoryStream();
                using (var originalReader = dataSet.CreateDataReader())
                {
                    DataSerializer.Serialize(stream, originalReader);

                    stream.Seek(0, SeekOrigin.Begin);

                    using (var reader = DataSerializer.Deserialize(stream))
                        deserializedDataSet.Load(reader, LoadOption.OverwriteChanges, "DimCustomer", "DimProduct");
                }
            }

            [Test]
            public void All_tables_should_have_the_same_contents()
            {
                AssertHelper.AssertContentsEqual(dataSet, deserializedDataSet);
            }
        }

        [TestFixture]
        public class When_advancing_to_the_next_result_set_but_still_in_the_middle_of_the_current_one
        {
            private DataSet dataSet;
            private IDataReader reader;
            private Stream stream;

            [TestFixtureSetUp]
            public void TestFixtureSetUp()
            {
                var a = new[]
                            {
                                new object[] {"Number"},
                                new object[] {0},
                                new object[] {1},
                                new object[] {2},
                                new object[] {3},
                                new object[] {4},
                                new object[] {5},
                                new object[] {6},
                                new object[] {7},
                                new object[] {8},
                                new object[] {9},
                            };

                var b = new[]
                            {
                                new object[] {"Letter"},
                                new object[] {"A"},
                                new object[] {"B"},
                                new object[] {"C"},
                                new object[] {"D"},
                                new object[] {"E"},
                            };

                dataSet = new DataSet();

                var tableWithNoRows = new DataTable();
                tableWithNoRows.Columns.Add("ColumnA", typeof (Guid));

                var c = new[]
                            {
                                new object[] {"Number"},
                                new object[] {9},
                                new object[] {8}
                            };
                
                dataSet.Tables.Add(TestData.FromMatrix(a));
                dataSet.Tables.Add(TestData.FromMatrix(b));
                dataSet.Tables.Add(tableWithNoRows);
                dataSet.Tables.Add(new DataTable());
                dataSet.Tables.Add(TestData.FromMatrix(c));

                stream = new MemoryStream();
                using (var originalReader = dataSet.CreateDataReader())
                {
                    DataSerializer.Serialize(stream, originalReader);

                    stream.Seek(0, SeekOrigin.Begin);

                    reader = DataSerializer.Deserialize(stream);

                }
            }

            [TestFixtureTearDown]
            public void TestFixtureTearDown()
            {
                reader.Dispose();
                stream.Dispose();
            }

            [Test]
            public void Should_produce_the_same_number_of_tables()
            {
                reader.Read().Should().Be.True();
                reader.GetInt32(0).Should().Be(0);
                reader.Read().Should().Be.True();
                reader.GetInt32(0).Should().Be(1);
                reader.Read().Should().Be.True();
                reader.GetInt32(0).Should().Be(2);

                reader.NextResult().Should().Be.True();

                reader.Read().Should().Be.True();
                reader.GetString(0).Should().Be("A");
                reader.Read().Should().Be.True();
                reader.GetString(0).Should().Be("B");

                reader.NextResult().Should().Be.True();
                reader.Read().Should().Be.False();

                reader.NextResult().Should().Be.True();
                reader.Read().Should().Be.False();
                reader.NextResult().Should().Be.True();

                reader.Read().Should().Be.True();
                reader.GetInt32(0).Should().Be(9);
                reader.Read().Should().Be.True();
                reader.GetInt32(0).Should().Be(8);
                reader.Read().Should().Be.False();
                reader.NextResult().Should().Be.False();
            }
        }

        [TestFixture]
        public class When_a_table_with_no_columns_is_immediately_followed_by_a_non_empty_one
        {
            private DataSet dataSet;
            private IDataReader reader;
            private Stream stream;

            [TestFixtureSetUp]
            public void TestFixtureSetUp()
            {
                dataSet = new DataSet();

                var tableWithNoRows = new DataTable();
                var c = new[]
                            {
                                new object[] {"Number"},
                                new object[] {9},
                                new object[] {8}
                            };

                dataSet.Tables.Add(tableWithNoRows);
                dataSet.Tables.Add(TestData.FromMatrix(c));

                stream = new MemoryStream();
                using (var originalReader = dataSet.CreateDataReader())
                {
                    DataSerializer.Serialize(stream, originalReader);

                    stream.Seek(0, SeekOrigin.Begin);

                    reader = DataSerializer.Deserialize(stream);
                }
            }

            [TestFixtureTearDown]
            public void TestFixtureTearDown()
            {
                reader.Dispose();
                stream.Dispose();
            }

            [Test]
            public void Should_produce_the_same_number_of_tables()
            {
                reader.Read().Should().Be.False();
                reader.NextResult().Should().Be.True();

                reader.Read().Should().Be.True();
                reader.GetInt32(0).Should().Be(9);
                reader.Read().Should().Be.True();
                reader.GetInt32(0).Should().Be(8);
                reader.Read().Should().Be.False();
                reader.NextResult().Should().Be.False();
            }
        }

        [TestFixture]
        public class When_a_table_with_no_rows_is_immediately_followed_by_a_non_empty_one
        {
            private DataSet dataSet;
            private IDataReader reader;
            private Stream stream;

            [TestFixtureSetUp]
            public void TestFixtureSetUp()
            {
                dataSet = new DataSet();

                var tableWithNoRows = new DataTable();
                tableWithNoRows.Columns.Add("Something", typeof (DateTime));
                var c = new[]
                            {
                                new object[] {"Number"},
                                new object[] {9},
                                new object[] {8}
                            };

                dataSet.Tables.Add(tableWithNoRows);
                dataSet.Tables.Add(TestData.FromMatrix(c));

                stream = new MemoryStream();
                using (var originalReader = dataSet.CreateDataReader())
                {
                    DataSerializer.Serialize(stream, originalReader);

                    stream.Seek(0, SeekOrigin.Begin);

                    reader = DataSerializer.Deserialize(stream);
                }
            }

            [TestFixtureTearDown]
            public void TestFixtureTearDown()
            {
                reader.Dispose();
                stream.Dispose();
            }

            [Test]
            public void Should_produce_the_same_number_of_tables()
            {
                reader.Read().Should().Be.False();
                reader.NextResult().Should().Be.True();

                reader.Read().Should().Be.True();
                reader.GetInt32(0).Should().Be(9);
                reader.Read().Should().Be.True();
                reader.GetInt32(0).Should().Be(8);
                reader.Read().Should().Be.False();
                reader.NextResult().Should().Be.False();
            }
        }
    }

}