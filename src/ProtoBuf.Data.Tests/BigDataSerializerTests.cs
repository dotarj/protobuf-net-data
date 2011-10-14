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

using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using NUnit.Framework;
using SharpTestsEx;

namespace ProtoBuf.Data.Tests
{
    public class BigDataSerializerTests
    {
        [TestFixture]
        public class When_serializing_a_really_big_data_table_to_a_buffer_and_back
        {
            DataTable originalTable;
            DataTable deserializedTable;

            [TestFixtureSetUp]
            public void TestFixtureSetUp()
            {

                var connectionString = new SqlConnectionStringBuilder
                {
                    DataSource = @".\SQLEXPRESS",
                    InitialCatalog = "AdventureWorksDW2008R2",
                    IntegratedSecurity = true
                };

                originalTable = new DataTable();
                using (var connection = new SqlConnection(connectionString.ConnectionString))
                {
                    connection.Open();
                    using (var transaction = connection.BeginTransaction())
                    using (var command = connection.CreateCommand())
                    {
                        command.Transaction = transaction;

                        command.CommandText = "SELECT * FROM DimCustomer";

                        using (var dataReader = command.ExecuteReader())
                            originalTable.Load(dataReader);
                    }
                }

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
            public void Should_produce_the_same_number_of_rows()
            {
                deserializedTable.Rows.Count.Should().Be(originalTable.Rows.Count);
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
            public void Should_serialize_row_values_correctly()
            {
                TestHelper.AssertRowValuesEqual(originalTable, deserializedTable);
            }
        }
    }
}