// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Data;
using System.IO;
using Moq;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public class DataSerializerTests
    {
        public class When_serializing_a_data_table_to_a_buffer_and_back
        {
            private readonly DataTable originalTable;
            private readonly DataTable deserializedTable;

            public When_serializing_a_data_table_to_a_buffer_and_back()
            {
                this.originalTable = TestData.SmallDataTable();
                this.deserializedTable = TestHelper.SerializeAndDeserialize(this.originalTable);
            }

            [Fact]
            public void Should_have_the_same_contents_as_the_original()
            {
                TestHelper.AssertContentsEqual(this.originalTable, this.deserializedTable);
            }
        }

        public class When_serializing_a_data_table_with_null_options
        {
            private readonly DataTable originalTable;
            private readonly DataTable deserializedTable;

            public When_serializing_a_data_table_with_null_options()
            {
                this.originalTable = TestData.SmallDataTable();
                this.deserializedTable = TestHelper.SerializeAndDeserialize(this.originalTable, null);
            }

            [Fact]
            public void Should_have_the_same_contents_as_the_original_and_not_throw_any_exception()
            {
                TestHelper.AssertContentsEqual(this.originalTable, this.deserializedTable);
            }
        }

        public class When_serializing_an_unsupported_type
        {
            private readonly DataTable originalTable;

            public When_serializing_an_unsupported_type()
            {
                this.originalTable = new DataTable();
                this.originalTable.Columns.Add("Foo", typeof(Foo));
                this.originalTable.Rows.Add(new Foo());
            }

            [Fact]
            public void Should_throw_an_exception()
            {
                using (var originalReader = this.originalTable.CreateDataReader())
                {
                    Assert.Throws<UnsupportedColumnTypeException>(() => DataSerializer.Serialize(Stream.Null, originalReader));
                }
            }

            private class Foo
            {
            }
        }

        public class When_serializing_a_data_table_with_no_rows
        {
            private readonly DataTable originalTable;
            private readonly DataTable deserializedTable;

            public When_serializing_a_data_table_with_no_rows()
            {
                this.originalTable = new DataTable();
                this.originalTable.Columns.Add("ColumnA", typeof(int));

                this.deserializedTable = TestHelper.SerializeAndDeserialize(this.originalTable);
            }

            [Fact]
            public void Should_have_the_same_contents_as_the_original()
            {
                TestHelper.AssertContentsEqual(this.originalTable, this.deserializedTable);
            }
        }

        public class When_serializing_a_data_table_with_no_columns_or_rows
        {
            private readonly DataTable originalTable;
            private readonly DataTable deserializedTable;

            public When_serializing_a_data_table_with_no_columns_or_rows()
            {
                this.originalTable = new DataTable();

                this.deserializedTable = TestHelper.SerializeAndDeserialize(this.originalTable);
            }

            [Fact]
            public void Should_have_the_same_contents_as_the_original()
            {
                TestHelper.AssertContentsEqual(this.originalTable, this.deserializedTable);
            }
        }

        public class When_serializing_a_data_table_with_a_computed_column
        {
            private DataTable originalTable;
            private DataTable deserializedTable;

            public When_serializing_a_data_table_with_a_computed_column()
            {
                var matrix = new[]
                                 {
                                     new object[] { "A", "B" },
                                     new object[] { 1, 2 },
                                     new object[] { 10, 20 },
                                 };

                this.originalTable = TestData.FromMatrix(matrix);

                var computed = TestData.FromMatrix(matrix);
                computed.Columns.Add(new DataColumn("C", typeof(int), "A+B"));

                this.deserializedTable = TestHelper.SerializeAndDeserialize(computed);
            }

            [Fact]
            public void Should_ignore_the_computed_column_by_default()
            {
                var isRunningOnMono = Type.GetType("Mono.Runtime") != null;

                if (!isRunningOnMono)
                {
                    TestHelper.AssertContentsEqual(this.originalTable, this.deserializedTable);
                }
            }
        }

        public class When_serializing_a_data_table_including_computed_columns
        {
            private DataTable originalTable;
            private DataTable deserializedTable;

            public When_serializing_a_data_table_including_computed_columns()
            {
                var matrix = new[]
                                 {
                                     new object[] { "A", "B" },
                                     new object[] { 1, 2 },
                                     new object[] { 10, 20 },
                                 };

                this.originalTable = TestData.FromMatrix(matrix);
                this.originalTable.Columns.Add(new DataColumn("C", typeof(int), "A+B"));

                this.deserializedTable = TestHelper.SerializeAndDeserialize(this.originalTable, new ProtoDataWriterOptions { IncludeComputedColumns = true });
            }

            [Fact]
            public void Should_ignore_the_computed_column()
            {
                TestHelper.AssertContentsEqual(this.originalTable, this.deserializedTable);
            }
        }

        public class When_serializing_a_data_reader_with_no_expression_column_in_its_schema_table
        {
            [Fact]
            public void Should_not_throw_any_exception()
            {
                // Fix for issue #12 https://github.com/rdingwall/protobuf-net-data/issues/12
                var matrix = new[]
                                 {
                                     new object[] { "A", "B" },
                                     new object[] { 1, 2 },
                                     new object[] { 10, 20 },
                                 };

                using (var table = TestData.FromMatrix(matrix))
                using (var reader = table.CreateDataReader())
                using (var schemaTable = reader.GetSchemaTable())
                {
                    var moqReader = new Mock<IDataReader>();
                    moqReader.Setup(r => r.GetSchemaTable()).Returns(schemaTable).Verifiable();
                    var originalReader = moqReader.Object;
                    schemaTable.Columns.Remove("Expression");

                    using (var stream = Stream.Null)
                    {
                        new ProtoDataWriter().Serialize(stream, originalReader);
                    }

                    moqReader.Verify();
                }
            }
        }

        public class When_serializing_a_data_table_with_zero_length_arrays
        {
            private DataTable originalTable;
            private DataTable deserializedTable;

            public When_serializing_a_data_table_with_zero_length_arrays()
            {
                var matrix = new[]
                                 {
                                     new object[] { "A", "B" },
                                     new object[] { new byte[0], "X" },
                                     new object[] { null, "Y" },
                                 };

                this.originalTable = TestData.FromMatrix(matrix);

                this.deserializedTable = TestHelper.SerializeAndDeserialize(this.originalTable);
            }

            [Fact]
            public void Should_ignore_the_computed_column()
            {
                TestHelper.AssertContentsEqual(this.originalTable, this.deserializedTable);
            }
        }
    }
}