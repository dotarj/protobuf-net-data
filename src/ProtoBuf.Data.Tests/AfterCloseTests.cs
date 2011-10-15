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
using NUnit.Framework;
using SharpTestsEx;

namespace ProtoBuf.Data.Tests
{
    public class AfterCloseTests
    {
        [TestFixture]
        public class When_the_reader_has_been_closed
        {
            private IDataReader reader;
            private MemoryStream stream;

            [TestFixtureSetUp]
            public void TestFixtureSetUp()
            {
                stream = new MemoryStream();
                using (var table = TestData.SmallDataTable())
                using (var tableReader = table.CreateDataReader())
                {
                    DataSerializer.Serialize(stream, tableReader);
                    stream.Seek(0, SeekOrigin.Begin);

                    reader = DataSerializer.Deserialize(stream);
                }

                reader.Close();
            }

            [TestFixtureTearDown]
            public void TestFixtureTearDown()
            {
                reader.Dispose();
                stream.Dispose();
            }

            [Test]
            public void IsClosed_should_be_set()
            {
                reader.IsClosed.Should().Be.True();
            }

            [Test]
            public void Should_not_throw_an_exception_if_you_try_to_close_it_twice()
            {
                reader.Close();
                reader.Close();
            }

            [Test]
            public void Should_throw_an_exception_if_you_try_to_get_the_schema_table()
            {
                Assert.Throws<InvalidOperationException>(() => reader.GetSchemaTable());
            }

            [Test]
            public void Should_throw_an_exception_if_you_try_to_get_the_field_count()
            {
                int dummy;
                Assert.Throws<InvalidOperationException>(() => dummy = reader.FieldCount);
            }

            [Test]
            public void Should_not_throw_an_exception_if_you_try_to_get_the_number_of_records_affected()
            {
                int dummy = reader.RecordsAffected;
            }

            [Test]
            public void Should_throw_an_exception_if_you_try_to_get_the_depth()
            {
                int dummy;
                Assert.Throws<InvalidOperationException>(() => dummy = reader.Depth);
            }

            [Test]
            public void Should_throw_an_exception_if_you_try_to_read()
            {
                Assert.Throws<InvalidOperationException>(() => reader.Read());
            }

            [Test]
            public void Should_throw_an_exception_if_you_try_to_move_to_the_next_result()
            {
                Assert.Throws<InvalidOperationException>(() => reader.NextResult());
            }

            [Test]
            public void Should_throw_an_exception_if_you_try_to_move_to_get_the_row()
            {
                var dummy = new object[10];
                Assert.Throws<InvalidOperationException>(() => reader.GetValues(dummy));
            }

            [Test]
            public void Should_throw_an_exception_if_you_try_to_get_a_column_name()
            {
                Assert.Throws<InvalidOperationException>(() => reader.GetName(0));
            }

            [Test]
            public void Should_throw_an_exception_if_you_try_to_get_a_columns_ordinal_position()
            {
                Assert.Throws<InvalidOperationException>(() => reader.GetOrdinal("foo"));
            }

            [Test]
            public void Should_throw_an_exception_if_you_try_to_get_a_columns_value()
            {
                Assert.Throws<InvalidOperationException>(() => reader.GetValue(0));
            }

            [Test]
            public void Should_throw_an_exception_if_you_try_to_get_a_columns_type()
            {
                Assert.Throws<InvalidOperationException>(() => reader.GetFieldType(0));
            }

            [Test]
            public void Should_throw_an_exception_if_you_try_to_get_a_columns_data_type_name()
            {
                Assert.Throws<InvalidOperationException>(() => reader.GetDataTypeName(0));
            }
        }
    }
}