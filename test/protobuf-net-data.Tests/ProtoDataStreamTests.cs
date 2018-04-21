// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
#if NET40
using Xunit.Extensions;
#endif

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataStreamTests
    {
        protected IDataReader CreateDataReader<TDataType>(TDataType value, int recordCount = 1)
        {
            var dataTable = new DataTable();

            dataTable.Columns.Add(typeof(TDataType).Name, typeof(TDataType));

            for (var i = 0; i < recordCount; i++)
            {
                dataTable.Rows.Add(value);
            }

            return dataTable.CreateDataReader();
        }

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
                    this.expectedBytes = memoryStream.GetTrimmedBuffer();
                }

                this.reader = testData.CreateDataReader();
                using (var stream = new ProtoDataStream(this.reader))
                using (var memoryStream = new MemoryStream())
                {
                    stream.CopyTo(memoryStream);
                    this.actualBytes = memoryStream.GetTrimmedBuffer();
                }
            }

            [Fact]
            public void It_should_be_binary_equal_to_the_data_serializer_version()
            {
                Assert.Equal(this.expectedBytes.Length, this.actualBytes.Length);
                Assert.Equal(this.expectedBytes, this.actualBytes);
            }

            [Fact]
            public void It_should_dispose_the_reader()
            {
                Assert.Throws<InvalidOperationException>(() => this.reader.Read());
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
                    this.expectedBytes = memoryStream.GetTrimmedBuffer();
                }

                this.reader = testData.CreateDataReader();
                using (var stream = new ProtoDataStream(this.reader))
                using (var memoryStream = new MemoryStream())
                {
                    stream.CopyTo(memoryStream);
                    this.actualBytes = memoryStream.GetTrimmedBuffer();
                }
            }

            [Fact]
            public void It_should_be_binary_equal_to_the_data_serializer_version()
            {
                Assert.Equal(this.expectedBytes.Length, this.actualBytes.Length);
                Assert.Equal(this.expectedBytes, this.actualBytes);
            }

            [Fact]
            public void It_should_dispose_the_reader()
            {
                Assert.Throws<InvalidOperationException>(() => this.reader.Read());
            }
        }

        public class When_reading_from_a_stream_with_varying_buffer_sizes
        {
            private byte[] expectedBytes;
            private DataTable testData;

            public When_reading_from_a_stream_with_varying_buffer_sizes()
            {
                this.testData = TestData.GenerateRandomDataTable(10, 10000);

                using (var reader = this.testData.CreateDataReader())
                using (var memoryStream = new MemoryStream())
                {
                    DataSerializer.Serialize(memoryStream, reader);
                    this.expectedBytes = memoryStream.GetTrimmedBuffer();
                }
            }

            [Fact]
            public void It_should_populate_the_buffer_correctly()
            {
                using (var reader = this.testData.CreateDataReader())
                using (var stream = new ProtoDataStream(reader))
                {
                    var buffer1 = new byte[16];
                    var buffer1Readed = stream.Read(buffer1, 0, buffer1.Length);
                    Assert.Equal(buffer1.Length, buffer1Readed);
                    Assert.Equal(this.expectedBytes.Take(buffer1.Length), buffer1);

                    buffer1Readed = stream.Read(buffer1, 0, 0);
                    Assert.Equal(0, buffer1Readed);

                    var buffer2 = new byte[16 * 1024];
                    var buffer2Readed = stream.Read(buffer2, 0, buffer2.Length);
                    Assert.Equal(buffer2.Length, buffer2Readed);
                    Assert.Equal(this.expectedBytes.Skip(buffer1.Length).Take(buffer2.Length), buffer2);
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

                this.reader = testData.CreateDataReader();
                this.stream = new ProtoDataStream(this.reader);

                var buffer = new byte[512];
                this.stream.Read(buffer, 0, buffer.Length);
                this.stream.Read(buffer, 0, buffer.Length);
                this.stream.Read(buffer, 0, buffer.Length);

                this.stream.Dispose();
            }

            [Fact]
            public void It_should_dispose_the_reader()
            {
                Assert.Throws<InvalidOperationException>(() => this.reader.Read());
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
                    this.expectedBytes = memoryStream.GetTrimmedBuffer();
                }

                this.reader = testData.CreateDataReader();
                using (var stream = new ProtoDataStream(this.reader))
                using (var memoryStream = new MemoryStream())
                {
                    stream.CopyTo(memoryStream);
                    this.actualBytes = memoryStream.GetTrimmedBuffer();
                }
            }

            [Fact]
            public void It_should_be_binary_equal_to_the_data_serializer_version()
            {
                Assert.Equal(this.expectedBytes.Length, this.actualBytes.Length);
                Assert.Equal(this.expectedBytes, this.actualBytes);
            }

            [Fact]
            public void It_should_dispose_the_reader()
            {
                Assert.Throws<InvalidOperationException>(() => this.reader.Read());
            }
        }

        public class When_copies_from_proto_stream_with_variuos_buffers_sizes
        {
            private byte[] expectedBytes;
            private DataTable testData;

            public When_copies_from_proto_stream_with_variuos_buffers_sizes()
            {
                this.testData = TestData.SmallDataTable();

                using (var r = this.testData.CreateDataReader())
                using (var memoryStream = new MemoryStream())
                {
                    DataSerializer.Serialize(memoryStream, r);
                    this.expectedBytes = memoryStream.GetTrimmedBuffer();
                }
            }

            ~When_copies_from_proto_stream_with_variuos_buffers_sizes()
            {
                this.testData.Dispose();
                this.testData = null;
                this.expectedBytes = null;
            }

#if !NET40

            [Theory]
            [ClassData(typeof(BuffersData))]
            public async Task It_should_copy_async_with_various_buffers(int protoBufferSize, int copyBufferSize)
            {
                byte[] actualBytes;
                using (var reader = this.testData.CreateDataReader())
                using (var stream = new ProtoDataStream(reader, protoBufferSize * 1024))
                using (var memoryStream = new MemoryStream())
                {
                    await stream.CopyToAsync(memoryStream, copyBufferSize * 1024);
                    actualBytes = memoryStream.GetTrimmedBuffer();
                }

                Assert.Equal(this.expectedBytes.LongLength, actualBytes.LongLength);
                Assert.Equal(this.expectedBytes, actualBytes);
            }
#endif

            [Theory]
            [ClassData(typeof(BuffersData))]
            public void It_should_copy_with_various_buffers(int protoBufferSize, int copyBufferSize)
            {
                byte[] actualBytes;
                using (var reader = this.testData.CreateDataReader())
                using (var stream = new ProtoDataStream(reader, protoBufferSize * 1024))
                using (var memoryStream = new MemoryStream())
                {
                    stream.CopyTo(memoryStream, copyBufferSize * 1024);
                    actualBytes = memoryStream.GetTrimmedBuffer();
                }

                Assert.Equal(this.expectedBytes.LongLength, actualBytes.LongLength);
                Assert.Equal(this.expectedBytes, actualBytes);
            }

            private class BuffersData : IEnumerable<object[]>
            {
                private readonly int[] protoBufferSizes = { 128, 137, 1024 };
                private readonly int[] copyBufferSizes = { 64, 75, 128, 512 };

                public IEnumerator<object[]> GetEnumerator()
                {
                    foreach (var protoBufferSize in this.protoBufferSizes)
                    {
                        foreach (var copyBufferSize in this.copyBufferSizes)
                        {
                            yield return new object[] { protoBufferSize, copyBufferSize };
                        }
                    }
                }

                IEnumerator IEnumerable.GetEnumerator() => this.GetEnumerator();
            }
        }

        public class When_reading_from_a_stream_containing_multiple_data_tables_with_big_buffer
        {
            private byte[] expectedBytes;
            private byte[] actualBytes;
            private IDataReader reader;

            public When_reading_from_a_stream_containing_multiple_data_tables_with_big_buffer()
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
                    this.expectedBytes = memoryStream.GetTrimmedBuffer();
                }

                this.reader = testData.CreateDataReader();
                using (var stream = new ProtoDataStream(this.reader, 50 * 1024 * 1024)) // 50 Mb buffer which
                using (var memoryStream = new MemoryStream())
                {
                    stream.CopyTo(memoryStream);
                    this.actualBytes = memoryStream.GetTrimmedBuffer();
                }
            }

            [Fact]
            public void It_should_be_binary_equal_to_the_data_serializer_version()
            {
                Assert.Equal(this.expectedBytes.LongLength, this.actualBytes.LongLength);
                Assert.Equal(this.expectedBytes, this.actualBytes);
            }

            [Fact]
            public void It_should_dispose_the_reader()
            {
                Assert.Throws<InvalidOperationException>(() => this.reader.Read());
            }
        }
    }
}