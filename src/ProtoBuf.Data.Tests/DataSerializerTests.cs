using System;
using System.Data;
using System.IO;
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

                originalTable = new DataTable();
                originalTable.Columns.Add("Birthday", typeof(DateTime));
                originalTable.Columns.Add("Age", typeof(int));
                originalTable.Columns.Add("Name", typeof(string));
                originalTable.Columns.Add("ID", typeof(Guid));
                originalTable.Rows.Add(DateTime.Today.Date, 42, "Foo", Guid.NewGuid());
                originalTable.Rows.Add(DateTime.Today.AddDays(-8).Date, null, "Bar", Guid.NewGuid());

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