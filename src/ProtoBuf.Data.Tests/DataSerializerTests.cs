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
using System.Collections.Generic;

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

                originalTable = new DataTable();
                originalTable.Columns.Add("Birthday", typeof(DateTime));
                originalTable.Columns.Add("Age", typeof(int));
                originalTable.Columns.Add("Name", typeof(string));
                originalTable.Columns.Add("ID", typeof(Guid));
                originalTable.Columns.Add("LastName", typeof(string));
                originalTable.Rows.Add(DateTime.Today.Date, 42, "Foo", Guid.NewGuid(), "sdfsdf");
                originalTable.Rows.Add(DateTime.Today.AddDays(-8).Date, null, "Bar", Guid.NewGuid(), "o2389uf");

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
                var originalNames = originalTable.Columns
                    .OfType<DataColumn>().Select(c => c.ColumnName).ToArray();

                var deserializedNames = deserializedTable.Columns
                    .OfType<DataColumn>().Select(c => c.ColumnName).ToArray();

                for (var i = 0; i < originalNames.Length; i++)
                    deserializedNames[i].Should().Be.EqualTo(originalNames[i]);
            }

            [Test]
            public void The_columns_should_all_have_the_same_types()
            {
                var originalTypes = originalTable.Columns
                    .OfType<DataColumn>().Select(c => c.DataType).ToArray();

                var deserializedTypes = deserializedTable.Columns
                    .OfType<DataColumn>().Select(c => c.DataType).ToArray();

                for (var i = 0; i < originalTypes.Length; i++)
                    deserializedTypes[i].Should().Be.EqualTo(originalTypes[i]);
            }

            [Test]
            public void Should_produce_the_same_number_of_rows()
            {
                deserializedTable.Rows.Count.Should().Be(originalTable.Rows.Count);
            }

            [Test]
            public void Should_serialize_row_values_correctly()
            {
                for (int i = 0; i < originalTable.Rows.Count; i++)
                {
                    var originalValues = originalTable.Rows[i].ItemArray;
                    var deserializedValues = deserializedTable.Rows[i].ItemArray;
                    deserializedValues.Should().Have.SameSequenceAs(originalValues);
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
    }
}