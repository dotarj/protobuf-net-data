// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Data;
using System.IO;
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
                this.dataSet = TestData.DataSetFromSql("SELECT TOP 25 * FROM DimCustomer; SELECT TOP 42 * FROM DimProduct;", "DimCustomer", "DimProduct");

                this.deserializedDataSet = TestHelper.SerializeAndDeserialize(this.dataSet, "DimCustomer", "DimProduct");
            }

            [Fact(Skip = "Integration")]
            public void All_tables_should_have_the_same_contents()
            {
                TestHelper.AssertContentsEqual(this.dataSet, this.deserializedDataSet);
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
                                new object[] { "Number" },
                                new object[] { 0 },
                                new object[] { 1 },
                                new object[] { 2 },
                                new object[] { 3 },
                                new object[] { 4 },
                                new object[] { 5 },
                                new object[] { 6 },
                                new object[] { 7 },
                                new object[] { 8 },
                                new object[] { 9 },
                            };

                var b = new[]
                            {
                                new object[] { "Letter", "Number" },
                                new object[] { "A", 9 },
                                new object[] { "B", 8 },
                                new object[] { "C", 7 },
                                new object[] { "D", 6 },
                                new object[] { "E", 5 },
                            };

                this.dataSet = new DataSet();

                var tableWithNoRows = new DataTable();
                tableWithNoRows.Columns.Add("ColumnA", typeof(Guid));

                var c = new[]
                            {
                                new object[] { "Number" },
                                new object[] { 9 },
                                new object[] { 8 }
                            };

                this.dataSet.Tables.Add(TestData.FromMatrix(a));
                this.dataSet.Tables.Add(TestData.FromMatrix(b));
                this.dataSet.Tables.Add(tableWithNoRows);
                this.dataSet.Tables.Add(new DataTable());
                this.dataSet.Tables.Add(TestData.FromMatrix(c));

                this.stream = new MemoryStream();
                using (var originalReader = this.dataSet.CreateDataReader())
                {
                    DataSerializer.Serialize(this.stream, originalReader);

                    this.stream.Seek(0, SeekOrigin.Begin);

                    this.reader = DataSerializer.Deserialize(this.stream);
                }
            }

            public void Dispose()
            {
                this.reader.Dispose();
                this.stream.Dispose();
            }

            [Fact]
            public void Should_produce_the_same_number_of_tables()
            {
                Assert.Equal(1, this.reader.GetSchemaTable().Rows.Count);
                Assert.True(this.reader.Read());
                Assert.Equal(0, this.reader.GetInt32(0));
                Assert.True(this.reader.Read());
                Assert.Equal(1, this.reader.GetInt32(0));
                Assert.True(this.reader.Read());
                Assert.Equal(2, this.reader.GetInt32(0));
                Assert.Equal(1, this.reader.GetSchemaTable().Rows.Count);

                Assert.True(this.reader.NextResult());

                Assert.Equal(2, this.reader.GetSchemaTable().Rows.Count);
                Assert.True(this.reader.Read());
                Assert.Equal("A", this.reader.GetString(0));
                Assert.True(this.reader.Read());
                Assert.Equal("B", this.reader.GetString(0));
                Assert.Equal(2, this.reader.GetSchemaTable().Rows.Count);

                Assert.True(this.reader.NextResult());
                Assert.Equal(1, this.reader.GetSchemaTable().Rows.Count);
                Assert.False(this.reader.Read());
                Assert.Equal(1, this.reader.GetSchemaTable().Rows.Count);

                Assert.True(this.reader.NextResult());
                Assert.Equal(0, this.reader.GetSchemaTable().Rows.Count);
                Assert.False(this.reader.Read());
                Assert.Equal(0, this.reader.GetSchemaTable().Rows.Count);
                Assert.True(this.reader.NextResult());

                Assert.Equal(1, this.reader.GetSchemaTable().Rows.Count);
                Assert.True(this.reader.Read());
                Assert.Equal(9, this.reader.GetInt32(0));
                Assert.True(this.reader.Read());
                Assert.Equal(8, this.reader.GetInt32(0));
                Assert.False(this.reader.Read());
                Assert.Equal(1, this.reader.GetSchemaTable().Rows.Count);
                Assert.False(this.reader.NextResult());
            }
        }

        public class When_a_table_with_no_columns_is_immediately_followed_by_a_non_empty_one : IDisposable
        {
            private DataSet dataSet;
            private IDataReader reader;
            private Stream stream;

            public When_a_table_with_no_columns_is_immediately_followed_by_a_non_empty_one()
            {
                this.dataSet = new DataSet();

                var tableWithNoRows = new DataTable();
                var c = new[]
                            {
                                new object[] { "Number", "Letter" },
                                new object[] { 9, "A" },
                                new object[] { 8, "B" }
                            };

                this.dataSet.Tables.Add(tableWithNoRows);
                this.dataSet.Tables.Add(TestData.FromMatrix(c));

                this.stream = new MemoryStream();
                using (var originalReader = this.dataSet.CreateDataReader())
                {
                    DataSerializer.Serialize(this.stream, originalReader);

                    this.stream.Seek(0, SeekOrigin.Begin);

                    this.reader = DataSerializer.Deserialize(this.stream);
                }
            }

            public void Dispose()
            {
                this.reader.Dispose();
                this.stream.Dispose();
            }

            [Fact]
            public void Should_produce_the_same_number_of_tables()
            {
                Assert.Equal(0, this.reader.GetSchemaTable().Rows.Count);
                Assert.False(this.reader.Read());
                Assert.Equal(0, this.reader.GetSchemaTable().Rows.Count);
                Assert.True(this.reader.NextResult());

                Assert.Equal(2, this.reader.GetSchemaTable().Rows.Count);
                Assert.True(this.reader.Read());
                Assert.Equal(9, this.reader.GetInt32(0));
                Assert.True(this.reader.Read());
                Assert.Equal(8, this.reader.GetInt32(0));
                Assert.False(this.reader.Read());
                Assert.Equal(2, this.reader.GetSchemaTable().Rows.Count);
                Assert.False(this.reader.NextResult());
            }
        }

        public class When_a_table_with_no_rows_is_immediately_followed_by_a_non_empty_one : IDisposable
        {
            private DataSet dataSet;
            private IDataReader reader;
            private Stream stream;

            public When_a_table_with_no_rows_is_immediately_followed_by_a_non_empty_one()
            {
                this.dataSet = new DataSet();

                var tableWithNoRows = new DataTable();
                tableWithNoRows.Columns.Add("Something", typeof(DateTime));
                var c = new[]
                            {
                                new object[] { "Number", "Letter" },
                                new object[] { 9, "Z" },
                                new object[] { 8, "Y" }
                            };

                this.dataSet.Tables.Add(tableWithNoRows);
                this.dataSet.Tables.Add(TestData.FromMatrix(c));

                this.stream = new MemoryStream();
                using (var originalReader = this.dataSet.CreateDataReader())
                {
                    DataSerializer.Serialize(this.stream, originalReader);

                    this.stream.Seek(0, SeekOrigin.Begin);

                    this.reader = DataSerializer.Deserialize(this.stream);
                }
            }

            public void Dispose()
            {
                this.reader.Dispose();
                this.stream.Dispose();
            }

            [Fact]
            public void Should_produce_the_same_number_of_tables()
            {
                Assert.Equal(1, this.reader.GetSchemaTable().Rows.Count);
                Assert.False(this.reader.Read());
                Assert.Equal(1, this.reader.GetSchemaTable().Rows.Count);
                Assert.True(this.reader.NextResult());

                Assert.Equal(2, this.reader.GetSchemaTable().Rows.Count);
                Assert.True(this.reader.Read());
                Assert.Equal(9, this.reader.GetInt32(0));
                Assert.True(this.reader.Read());
                Assert.Equal(8, this.reader.GetInt32(0));
                Assert.False(this.reader.Read());
                Assert.Equal(2, this.reader.GetSchemaTable().Rows.Count);
                Assert.False(this.reader.NextResult());
            }
        }
    }
}