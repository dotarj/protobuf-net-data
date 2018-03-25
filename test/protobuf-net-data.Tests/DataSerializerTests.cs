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
            DataTable originalTable;
            DataTable deserializedTable;

            public When_serializing_a_data_table_to_a_buffer_and_back()
            {
                originalTable = TestData.SmallDataTable();
                deserializedTable = TestHelper.SerializeAndDeserialize(originalTable);
            }

            [Fact]
            public void Should_have_the_same_contents_as_the_original()
            {
                TestHelper.AssertContentsEqual(originalTable, deserializedTable);
            }
        }

        public class When_serializing_a_data_table_with_null_options
        {
            DataTable originalTable;
            DataTable deserializedTable;

            public When_serializing_a_data_table_with_null_options()
            {
                originalTable = TestData.SmallDataTable();
                deserializedTable = TestHelper.SerializeAndDeserialize(originalTable, null);
            }

            [Fact]
            public void Should_have_the_same_contents_as_the_original_and_not_throw_any_exception()
            {
                TestHelper.AssertContentsEqual(originalTable, deserializedTable);
            }
        }

        public class When_serializing_an_unsupported_type
        {
            DataTable originalTable;

            class Foo { }

            public When_serializing_an_unsupported_type()
            {
                originalTable = new DataTable();
                originalTable.Columns.Add("Foo", typeof(Foo));
                originalTable.Rows.Add(new Foo());
            }

            [Fact]
            public void Should_throw_an_exception()
            {
                using (var originalReader = originalTable.CreateDataReader())
                    Assert.Throws<UnsupportedColumnTypeException>(() => DataSerializer.Serialize(Stream.Null, originalReader));
            }
        }

        public class When_serializing_a_data_table_with_no_rows
        {
            DataTable originalTable;
            DataTable deserializedTable;

            public When_serializing_a_data_table_with_no_rows()
            {
                originalTable = new DataTable();
                originalTable.Columns.Add("ColumnA", typeof(int));

                deserializedTable = TestHelper.SerializeAndDeserialize(originalTable);
            }

            [Fact]
            public void Should_have_the_same_contents_as_the_original()
            {
                TestHelper.AssertContentsEqual(originalTable, deserializedTable);
            }
        }

        public class When_serializing_a_data_table_with_no_columns_or_rows
        {
            DataTable originalTable;
            DataTable deserializedTable;

            public When_serializing_a_data_table_with_no_columns_or_rows()
            {
                originalTable = new DataTable();

                deserializedTable = TestHelper.SerializeAndDeserialize(originalTable);
            }

            [Fact]
            public void Should_have_the_same_contents_as_the_original()
            {
                TestHelper.AssertContentsEqual(originalTable, deserializedTable);
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
                                     new object[] {"A", "B"},
                                     new object[] {1, 2},
                                     new object[] {10, 20},
                                 };

                originalTable = TestData.FromMatrix(matrix);

                var computed = TestData.FromMatrix(matrix);
                computed.Columns.Add(new DataColumn("C", typeof(int), "A+B"));

                deserializedTable = TestHelper.SerializeAndDeserialize(computed);
            }

            [Fact]
            public void Should_ignore_the_computed_column_by_default()
            {
                TestHelper.AssertContentsEqual(originalTable, deserializedTable);
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
                                     new object[] {"A", "B"},
                                     new object[] {1, 2},
                                     new object[] {10, 20},
                                 };

                originalTable = TestData.FromMatrix(matrix);
                originalTable.Columns.Add(new DataColumn("C", typeof(int), "A+B"));

                deserializedTable = TestHelper.SerializeAndDeserialize(originalTable,
                    new ProtoDataWriterOptions { IncludeComputedColumns = true });
            }

            [Fact]
            public void Should_ignore_the_computed_column()
            {
                TestHelper.AssertContentsEqual(originalTable, deserializedTable);
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
                                     new object[] {"A", "B"},
                                     new object[] {1, 2},
                                     new object[] {10, 20},
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
                        new ProtoDataWriter().Serialize(stream, originalReader);

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
                                     new object[] {"A", "B"},
                                     new object[] { new byte[] {}, "X" },
                                     new object[] { null, "Y"},
                                 };

                originalTable = TestData.FromMatrix(matrix);

                deserializedTable = TestHelper.SerializeAndDeserialize(originalTable);
            }

            [Fact]
            public void Should_ignore_the_computed_column()
            {
                TestHelper.AssertContentsEqual(originalTable, deserializedTable);
            }
        }
    }
}