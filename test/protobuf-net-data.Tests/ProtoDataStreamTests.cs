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
using SharpTestsEx;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public class ProtoDataStreamTests
    {
        // Protobuf-net only flushes every 1024 bytes. So check we can
        // successfully write a stream smaller than that.
        public class When_reading_from_a_stream_less_than_protobuf_nets_flush_size
        {
            private byte[] expectedBytes;
            private byte[] actualBytes;
            private DataTableReader reader;
            
            public When_reading_from_a_stream_less_than_protobuf_nets_flush_size()
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

            [Fact]
            public void It_should_be_binary_equal_to_the_data_serializer_version()
            {
                actualBytes.Length.Should().Be.EqualTo(expectedBytes.Length);
                actualBytes.Should().Have.SameSequenceAs(expectedBytes);
            }

            [Fact]
            public void It_should_dispose_the_reader()
            {
                Assert.Throws<InvalidOperationException>(() => reader.Read());
            }
        }

        public class When_reading_from_a_stream
        {
            private byte[] expectedBytes;
            private byte[] actualBytes;
            private IDataReader reader;

            public When_reading_from_a_stream()
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

            [Fact]
            public void It_should_be_binary_equal_to_the_data_serializer_version()
            {
                //actualBytes.Length.Should().Be.EqualTo(expectedBytes.Length);
                actualBytes.Should().Have.SameSequenceAs(expectedBytes);
            }

            [Fact]
            public void It_should_dispose_the_reader()
            {
                Assert.Throws<InvalidOperationException>(() => reader.Read());
            }
        }

        public class When_reading_from_a_stream_with_varying_buffer_sizes
        {
            private byte[] expectedBytes;
            private DataTable testData;

            public When_reading_from_a_stream_with_varying_buffer_sizes()
            {
                testData = TestData.GenerateRandomDataTable(10, 10000);

                using (var reader = testData.CreateDataReader())
                using (var memoryStream = new MemoryStream())
                {
                    DataSerializer.Serialize(memoryStream, reader);
                    expectedBytes = memoryStream.GetTrimmedBuffer();
                }
            }

            [Fact]
            public void It_should_populate_the_buffer_correctly()
            {
                using (var reader = testData.CreateDataReader())
                using (var stream = new ProtoDataStream(reader))
                {
                    var buffer1 = new byte[16];
                    stream.Read(buffer1, 0, buffer1.Length).Should().Be(buffer1.Length);
                    buffer1.Should().Have.SameSequenceAs(expectedBytes.Take(buffer1.Length));

                    stream.Read(buffer1, 0, 0).Should().Be(0);

                    var buffer2 = new byte[16 * 1024];
                    stream.Read(buffer2, 0, buffer2.Length).Should().Be(buffer2.Length);
                    buffer2.Should().Have.SameSequenceAs(expectedBytes.Skip(buffer1.Length).Take(buffer2.Length));
                }
            }
        }

        public class When_disposing_the_read_before_all_the_data_has_been_read
        {
            private Stream stream;
            private IDataReader reader;

            public When_disposing_the_read_before_all_the_data_has_been_read()
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

            [Fact]
            public void It_should_dispose_the_reader()
            {
                Assert.Throws<InvalidOperationException>(() => reader.Read());
            }
        }

        public class When_reading_from_a_stream_containing_multiple_data_tables
        {
            private byte[] expectedBytes;
            private byte[] actualBytes;
            private IDataReader reader;

            public When_reading_from_a_stream_containing_multiple_data_tables()
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

            [Fact]
            public void It_should_be_binary_equal_to_the_data_serializer_version()
            {
                actualBytes.Length.Should().Be.EqualTo(expectedBytes.Length);
                actualBytes.Should().Have.SameSequenceAs(expectedBytes);
            }

            [Fact]
            public void It_should_dispose_the_reader()
            {
                Assert.Throws<InvalidOperationException>(() => reader.Read());
            }
        }
    }
}