using System;
using System.Data;
using System.IO;
using System.Linq;
using NUnit.Framework;
using SharpTestsEx;

namespace ProtoBuf.Data.Tests
{
    public class BackwardsCompatibilityTests
    {
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
                tableC.Columns.Add("Name", typeof(string));

                var tableD = new DataTable();
                tableD.Columns.Add("Value", typeof(int));
                tableD.Rows.Add(1);
                tableD.Rows.Add(2);
                tableD.Rows.Add(3);

                var dataSet = new DataSet();
                dataSet.Tables.AddRange(new[] { tableA, tableB, tableC, tableD });
                return dataSet;
            }

        const string TestFile = "BackwardsCompatbilityTest.bin";

        [TestFixture]
        public class When_reading
        {
            [Test]
            public void Should_retain_binary_compatibility_when_reading()
            {
                using (var expected = CreateTablesForBackwardsCompatibilityTest())
                using (var actual = new DataSet())
                {
                    using (var stream = File.OpenRead(TestFile))
                    using (var reader = DataSerializer.Deserialize(stream))
                        actual.Load(reader, LoadOption.OverwriteChanges, "A", "B", "C", "D");

                    AssertHelper.AssertContentsEqual(expected, actual);
                }
            }
        }

        [TestFixture]
        public class When_writing
        {
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
    }
}