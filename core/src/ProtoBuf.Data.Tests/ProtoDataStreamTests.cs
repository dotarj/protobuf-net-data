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
using System.Threading.Tasks;
using NUnit.Framework;
#if NET35
using ProtoBuf.Data.Tests.Extensions;
#endif

namespace ProtoBuf.Data.Tests
{
    public class ProtoDataStreamTests
    {
        // Protobuf-net only flushes every 1024 bytes. So check we can
        // successfully write a stream smaller than that.
        [TestFixture]
        public class When_reading_from_a_stream_less_than_protobuf_nets_flush_size
        {
            private byte[] expectedBytes;
            private byte[] actualBytes;
            private DataTableReader reader;

            [OneTimeSetUp]
            public void SetUp()
            {
                var testData = TestData.SmallDataTable();

                using (var r = testData.CreateDataReader())
                using (var memoryStream = new MemoryStream())
                {
                    DataSerializer.Serialize(memoryStream, r);
                    expectedBytes = memoryStream.GetTrimmedBuffer();
                }

                reader = testData.CreateDataReader();
                using (var stream = new ProtoDataStream(reader))
                using (var memoryStream = new MemoryStream())
                {
                    stream.CopyTo(memoryStream);
                    actualBytes = memoryStream.GetTrimmedBuffer();
                }
            }

            [Test]
            public void It_should_be_binary_equal_to_the_data_serializer_version()
            {
                Assert.AreEqual(this.expectedBytes.LongLength, this.actualBytes.LongLength);
                CollectionAssert.AreEqual(this.expectedBytes, this.actualBytes);
            }

            [Test]
            public void It_should_dispose_the_reader()
            {
                Assert.Throws<InvalidOperationException>(() => reader.Read());
            }
        }

        [TestFixture]
        public class When_copies_from_proto_stream_with_variuos_buffers_sizes
        {
            private byte[] expectedBytes;
            private DataTable testData;

            [OneTimeSetUp]
            public void SetUp()
            {
                testData = TestData.SmallDataTable();

                using (var r = testData.CreateDataReader())
                using (var memoryStream = new MemoryStream())
                {
                    DataSerializer.Serialize(memoryStream, r);
                    expectedBytes = memoryStream.GetTrimmedBuffer();
                }
            }

            [OneTimeTearDown]
            public void TearDown()
            {
                testData.Dispose();
                testData = null;
                expectedBytes = null;
            }

#if !NET40

            [Test]
            public async Task It_should_copy_async_with_various_buffers([Values(128, 137, 1024)]int protoBufferSize, [Values(64, 75, 128, 512)]int copyBufferSize)
            {

                byte[] actualBytes_;
                using (var reader = testData.CreateDataReader())
                using (var stream = new ProtoDataStream(reader, protoBufferSize * 1024))
                using (var memoryStream = new MemoryStream())
                {

                    await stream.CopyToAsync(memoryStream, copyBufferSize * 1024);
                    actualBytes_ = memoryStream.GetTrimmedBuffer();
                }

                Assert.AreEqual(expectedBytes.LongLength, actualBytes_.LongLength);
                CollectionAssert.AreEqual(expectedBytes, actualBytes_);
            }
#endif


            [Test]
            public void It_should_copy_with_various_buffers([Values(128, 137, 1024)]int protoBufferSize, [Values(64, 75, 128, 512)]int copyBufferSize)
            {
                byte[] actualBytes_;
                using (var reader = testData.CreateDataReader())
                using (var stream = new ProtoDataStream(reader, protoBufferSize * 1024))
                using (var memoryStream = new MemoryStream())
                {

                    stream.CopyTo(memoryStream, copyBufferSize * 1024);
                    actualBytes_ = memoryStream.GetTrimmedBuffer();
                }

                Assert.AreEqual(expectedBytes.LongLength, actualBytes_.LongLength);
                CollectionAssert.AreEqual(expectedBytes, actualBytes_);
            }
        }

        [TestFixture]
        public class When_reading_from_a_stream
        {
            private byte[] expectedBytes;
            private byte[] actualBytes;
            private IDataReader reader;

            [OneTimeSetUp]
            public void SetUp()
            {
                var testData = TestData.GenerateRandomDataTable(10, 10000);

                using (var r = testData.CreateDataReader())
                using (var memoryStream = new MemoryStream())
                {
                    DataSerializer.Serialize(memoryStream, r);
                    expectedBytes = memoryStream.GetTrimmedBuffer();
                }

                reader = testData.CreateDataReader();
                using (var stream = new ProtoDataStream(reader))
                using (var memoryStream = new MemoryStream())
                {
                    stream.CopyTo(memoryStream);
                    actualBytes = memoryStream.GetTrimmedBuffer();
                }
            }

            [Test]
            public void It_should_be_binary_equal_to_the_data_serializer_version()
            {
                //actualBytes.Length.Should().Be.EqualTo(expectedBytes.Length);
                CollectionAssert.AreEqual(this.expectedBytes, this.actualBytes);
            }

            [Test]
            public void It_should_dispose_the_reader()
            {
                Assert.Throws<InvalidOperationException>(() => reader.Read());
            }
        }

        [TestFixture]
        public class When_reading_from_a_stream_with_varying_buffer_sizes
        {
            private byte[] expectedBytes;
            private DataTable testData;

            [OneTimeSetUp]
            public void SetUp()
            {
                testData = TestData.GenerateRandomDataTable(10, 10000);

                using (var reader = testData.CreateDataReader())
                using (var memoryStream = new MemoryStream())
                {
                    DataSerializer.Serialize(memoryStream, reader);
                    expectedBytes = memoryStream.GetTrimmedBuffer();
                }
            }

            [Test]
            public void It_should_populate_the_buffer_correctly()
            {
                using (var reader = testData.CreateDataReader())
                using (var stream = new ProtoDataStream(reader))
                {
                    var buffer1 = new byte[16];
                    var readedBytesFromBuffer1 = stream.Read(buffer1, 0, buffer1.Length);
                    Assert.AreEqual(buffer1.Length, readedBytesFromBuffer1);
                    CollectionAssert.AreEqual(this.expectedBytes.Take(buffer1.Length), buffer1);

                    var readZeroBytes = stream.Read(buffer1, 0, 0);
                    Assert.Zero(readZeroBytes);

                    var buffer2 = new byte[16 * 1024];
                    var readedBytesFromBuffer2 = stream.Read(buffer2, 0, buffer2.Length);

                    Assert.AreEqual(buffer2.Length, readedBytesFromBuffer2);
                    CollectionAssert.AreEqual(expectedBytes.Skip(buffer1.Length).Take(buffer2.Length), buffer2);
                }
            }
        }

        [TestFixture]
        public class When_disposing_the_read_before_all_the_data_has_been_read
        {
            private Stream stream;
            private IDataReader reader;

            [OneTimeSetUp]
            public void SetUp()
            {
                var testData = TestData.GenerateRandomDataTable(10, 10000);

                reader = testData.CreateDataReader();
                stream = new ProtoDataStream(reader);

                var buffer = new byte[512];
                stream.Read(buffer, 0, buffer.Length);
                stream.Read(buffer, 0, buffer.Length);
                stream.Read(buffer, 0, buffer.Length);

                stream.Dispose();
            }

            [Test]
            public void It_should_dispose_the_reader()
            {
                Assert.Throws<InvalidOperationException>(() => reader.Read());
            }
        }

        [TestFixture]
        public class When_reading_from_a_stream_containing_multiple_data_tables
        {
            private byte[] expectedBytes;
            private byte[] actualBytes;
            private IDataReader reader;

            [OneTimeSetUp]
            public void SetUp()
            {
                var testData = new DataSet
                {
                    Tables =
                            {
                                TestData.GenerateRandomDataTable(10, 50),
                                TestData.GenerateRandomDataTable(5, 100),
                                TestData.GenerateRandomDataTable(20, 10000)
                            }
                };

                using (var r = testData.CreateDataReader())
                using (var memoryStream = new MemoryStream())
                {
                    DataSerializer.Serialize(memoryStream, r);
                    expectedBytes = memoryStream.GetTrimmedBuffer();
                }

                reader = testData.CreateDataReader();
                using (var stream = new ProtoDataStream(reader))
                using (var memoryStream = new MemoryStream())
                {
                    stream.CopyTo(memoryStream);
                    actualBytes = memoryStream.GetTrimmedBuffer();
                }
            }

            [Test]
            public void It_should_be_binary_equal_to_the_data_serializer_version()
            {
                Assert.AreEqual(this.expectedBytes.LongLength, this.actualBytes.LongLength);
                CollectionAssert.AreEqual(this.expectedBytes, this.actualBytes);
            }

            [Test]
            public void It_should_dispose_the_reader()
            {
                Assert.Throws<InvalidOperationException>(() => reader.Read());
            }
        }

        [TestFixture()]
        public class When_reading_from_a_stream_containing_multiple_data_tables_with_big_buffer
        {
            private byte[] expectedBytes;
            private byte[] actualBytes;
            private IDataReader reader;

            [OneTimeSetUp]
            public void SetUp()
            {
                var testData = new DataSet
                {
                    Tables =
                        {
                                TestData.GenerateRandomDataTable(10, 50),
                                TestData.GenerateRandomDataTable(5, 100),
                                TestData.GenerateRandomDataTable(20, 10000)
                        }
                };

                using (var r = testData.CreateDataReader())
                using (var memoryStream = new MemoryStream())
                {
                    DataSerializer.Serialize(memoryStream, r);
                    expectedBytes = memoryStream.GetTrimmedBuffer();
                }

                reader = testData.CreateDataReader();
                using (var stream = new ProtoDataStream(reader, 50 * 1024 * 1024))//50 Mb buffer which 
                using (var memoryStream = new MemoryStream())
                {
                    stream.CopyTo(memoryStream);
                    actualBytes = memoryStream.GetTrimmedBuffer();
                }
            }

            [Test]
            public void It_should_be_binary_equal_to_the_data_serializer_version()
            {
                Assert.AreEqual(this.expectedBytes.LongLength, this.actualBytes.LongLength);
                CollectionAssert.AreEqual(this.expectedBytes, this.actualBytes);
            }

            [Test]
            public void It_should_dispose_the_reader()
            {
                Assert.Throws<InvalidOperationException>(() => reader.Read());
            }
        }
    }
}