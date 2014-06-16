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
using System.Linq;
using NUnit.Framework;
using SharpTestsEx;

namespace ProtoBuf.Data.Tests
{
    // For coverage, these tests should exercise all features of this library.
    public class BackwardsCompatibilityTests
    {
        static DataSet CreateTablesForBackwardsCompatibilityTest()
        {
            var tableA = new DataTable("A");
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

            var tableB = new DataTable("B");
            tableB.Columns.Add("Name", typeof(string));

            var tableC = new DataTable("C");
            tableC.Columns.Add("Value", typeof(int));
            tableC.Rows.Add(1);
            tableC.Rows.Add(2);
            tableC.Rows.Add(3);

            var tableD = new DataTable("D");
            tableD.Columns.Add("ID", typeof(int));
            tableD.Rows.Add(42);
            tableD.Rows.Add(99);

            var dataSet = new DataSet();
            dataSet.Tables.AddRange(new[] { tableA, tableB, tableC, tableD });
            return dataSet;
        }

        /// <summary>
        /// Version with zero-length arrays serialized as null.
        /// </summary>
        const string PreviousVersionTestFile = "OldBackwardsCompatbilityTest.bin";

        const string TestFile = "BackwardsCompatbilityTest.bin";

        [TestFixture]
        public class When_reading
        {
            [Test]
            public void Should_retain_binary_compatibility_when_reading()
            {
                using (DataSet expected = CreateTablesForBackwardsCompatibilityTest())
                using (DataSet actual = new DataSet())
                {
                    using (FileStream stream = File.OpenRead(TestFile))
                    using (IDataReader reader = DataSerializer.Deserialize(stream))
                        actual.Load(reader, LoadOption.PreserveChanges, "A", "B", "C", "D");

                    actual.HasErrors.Should().Be.False();

                    TestHelper.AssertContentsEqual(expected, actual);
                }
            }
        }

        [TestFixture]
        public class When_writing
        {
            [Test]
            public void Should_retain_binary_compatibility_when_writing()
            {
                using (DataSet dataSet = CreateTablesForBackwardsCompatibilityTest())
                using (var stream = new MemoryStream())
                {
                    using (DataTableReader reader = dataSet.CreateDataReader())
                    {
                        DataSerializer.Serialize(stream, reader);
                    }

                    byte[] expected = File.ReadAllBytes(TestFile);

                    stream.Seek(0, SeekOrigin.Begin);

                    stream.GetBuffer().Take(expected.Length).Should().Have.SameSequenceAs(expected);
                }
            }

            [Test]
            public void Should_retain_binary_compatibility_with_previous_versions_when_writing()
            {
                using (DataSet dataSet = CreateTablesForBackwardsCompatibilityTest())
                using (var stream = new MemoryStream())
                {
                    using (DataTableReader reader = dataSet.CreateDataReader())
                    {
                        var options = new ProtoDataWriterOptions { SerializeEmptyArraysAsNull = true };
                        DataSerializer.Serialize(stream, reader, options);
                    }

                    byte[] expected = File.ReadAllBytes(PreviousVersionTestFile);

                    stream.Seek(0, SeekOrigin.Begin);

                    stream.GetBuffer().Take(expected.Length).Should().Have.SameSequenceAs(expected);
                }
            }

            //[Test]
            [Ignore("Only when our binary format changes (and we don't care about breaking old versions).")]
            public void RegenerateTestFile()
            {
                using (DataSet dataSet = CreateTablesForBackwardsCompatibilityTest())
                using (FileStream stream = new FileStream(Path.Combine(@"..\..\", TestFile), FileMode.Create))
                {
                    using (DataTableReader reader = dataSet.CreateDataReader())
                        DataSerializer.Serialize(stream, reader);
                }
            }
        }
    }
}