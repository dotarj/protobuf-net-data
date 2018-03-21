// Copyright 2012 Richard Dingwall - http://richarddingwall.name
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
using SharpTestsEx;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public class MultipleDataTableTests
    {
        [Trait("Category", "Integration")]
        public class When_serializing_a_datareader_with_multiple_result_sets
        {
            private DataSet dataSet;
            private DataSet deserializedDataSet;

            public When_serializing_a_datareader_with_multiple_result_sets()
            {
                dataSet = TestData.DataSetFromSql("SELECT TOP 25 * FROM DimCustomer; SELECT TOP 42 * FROM DimProduct;",
                                                  "DimCustomer", "DimProduct");

                deserializedDataSet = TestHelper.SerializeAndDeserialize(dataSet, "DimCustomer", "DimProduct");
            }

            [Fact(Skip = "Integration")]
            public void All_tables_should_have_the_same_contents()
            {
                TestHelper.AssertContentsEqual(dataSet, deserializedDataSet);
            }
        }

        public class When_advancing_to_the_next_result_set_but_still_in_the_middle_of_the_current_one : IDisposable
        {
            private DataSet dataSet;
            private IDataReader reader;
            private Stream stream;

            public When_advancing_to_the_next_result_set_but_still_in_the_middle_of_the_current_one()
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
                                new object[] {"Letter", "Number"},
                                new object[] {"A", 9},
                                new object[] {"B", 8},
                                new object[] {"C", 7},
                                new object[] {"D", 6},
                                new object[] {"E", 5},
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

            public void Dispose()
            {
                reader.Dispose();
                stream.Dispose();
            }

            [Fact]
            public void Should_produce_the_same_number_of_tables()
            {
                reader.GetSchemaTable().Rows.Count.Should().Be(1);
                reader.Read().Should().Be.True();
                reader.GetInt32(0).Should().Be(0);
                reader.Read().Should().Be.True();
                reader.GetInt32(0).Should().Be(1);
                reader.Read().Should().Be.True();
                reader.GetInt32(0).Should().Be(2);
                reader.GetSchemaTable().Rows.Count.Should().Be(1);

                reader.NextResult().Should().Be.True();


                reader.GetSchemaTable().Rows.Count.Should().Be(2);
                reader.Read().Should().Be.True();
                reader.GetString(0).Should().Be("A");
                reader.Read().Should().Be.True();
                reader.GetString(0).Should().Be("B");
                reader.GetSchemaTable().Rows.Count.Should().Be(2);

                reader.NextResult().Should().Be.True();
                reader.GetSchemaTable().Rows.Count.Should().Be(1);
                reader.Read().Should().Be.False();
                reader.GetSchemaTable().Rows.Count.Should().Be(1);

                reader.NextResult().Should().Be.True();
                reader.GetSchemaTable().Rows.Count.Should().Be(0);
                reader.Read().Should().Be.False();
                reader.GetSchemaTable().Rows.Count.Should().Be(0);
                reader.NextResult().Should().Be.True();

                reader.GetSchemaTable().Rows.Count.Should().Be(1);
                reader.Read().Should().Be.True();
                reader.GetInt32(0).Should().Be(9);
                reader.Read().Should().Be.True();
                reader.GetInt32(0).Should().Be(8);
                reader.Read().Should().Be.False();
                reader.GetSchemaTable().Rows.Count.Should().Be(1);
                reader.NextResult().Should().Be.False();
            }
        }

        public class When_a_table_with_no_columns_is_immediately_followed_by_a_non_empty_one : IDisposable
        {
            private DataSet dataSet;
            private IDataReader reader;
            private Stream stream;

            public When_a_table_with_no_columns_is_immediately_followed_by_a_non_empty_one()
            {
                dataSet = new DataSet();

                var tableWithNoRows = new DataTable();
                var c = new[]
                            {
                                new object[] {"Number", "Letter"},
                                new object[] {9, "A"},
                                new object[] {8, "B"}
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

            public void Dispose()
            {
                reader.Dispose();
                stream.Dispose();
            }

            [Fact]
            public void Should_produce_the_same_number_of_tables()
            {
                reader.GetSchemaTable().Rows.Count.Should().Be(0);
                reader.Read().Should().Be.False();
                reader.GetSchemaTable().Rows.Count.Should().Be(0);
                reader.NextResult().Should().Be.True();

                reader.GetSchemaTable().Rows.Count.Should().Be(2);
                reader.Read().Should().Be.True();
                reader.GetInt32(0).Should().Be(9);
                reader.Read().Should().Be.True();
                reader.GetInt32(0).Should().Be(8);
                reader.Read().Should().Be.False();
                reader.GetSchemaTable().Rows.Count.Should().Be(2);
                reader.NextResult().Should().Be.False();
            }
        }

        public class When_a_table_with_no_rows_is_immediately_followed_by_a_non_empty_one : IDisposable
        {
            private DataSet dataSet;
            private IDataReader reader;
            private Stream stream;

            public When_a_table_with_no_rows_is_immediately_followed_by_a_non_empty_one()
            {
                dataSet = new DataSet();

                var tableWithNoRows = new DataTable();
                tableWithNoRows.Columns.Add("Something", typeof (DateTime));
                var c = new[]
                            {
                                new object[] {"Number", "Letter"},
                                new object[] {9, "Z"},
                                new object[] {8, "Y"}
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

            public void Dispose()
            {
                reader.Dispose();
                stream.Dispose();
            }

            [Fact]
            public void Should_produce_the_same_number_of_tables()
            {
                reader.GetSchemaTable().Rows.Count.Should().Be(1);
                reader.Read().Should().Be.False();
                reader.GetSchemaTable().Rows.Count.Should().Be(1);
                reader.NextResult().Should().Be.True();

                reader.GetSchemaTable().Rows.Count.Should().Be(2);
                reader.Read().Should().Be.True();
                reader.GetInt32(0).Should().Be(9);
                reader.Read().Should().Be.True();
                reader.GetInt32(0).Should().Be(8);
                reader.Read().Should().Be.False();
                reader.GetSchemaTable().Rows.Count.Should().Be(2);
                reader.NextResult().Should().Be.False();
            }
        }
    }

}