// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Data;
using System.IO;
using System.Reflection;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public class AfterCloseTests
    {
        public class When_the_reader_has_been_closed : IDisposable
        {
            private IDataReader reader;
            private MemoryStream stream;

            public When_the_reader_has_been_closed()
            {
                this.stream = new MemoryStream();
                using (var table = TestData.SmallDataTable())
                using (var tableReader = table.CreateDataReader())
                {
                    DataSerializer.Serialize(this.stream, tableReader);
                    this.stream.Seek(0, SeekOrigin.Begin);

                    this.reader = DataSerializer.Deserialize(this.stream);
                }

                this.reader.Close();
            }

            public void Dispose()
            {
                this.reader.Dispose();
                this.stream.Dispose();
            }

            [Fact]
            public void IsClosed_should_be_set()
            {
                Assert.True(this.reader.IsClosed);
            }

            [Fact]
            public void Should_not_throw_an_exception_if_you_try_to_close_it_twice()
            {
                this.reader.Close();
                this.reader.Close();
            }

            [Fact]
            public void Should_throw_an_exception_if_you_try_to_get_the_schema_table()
            {
                Assert.Throws<InvalidOperationException>(() => this.reader.GetSchemaTable());
            }

            [Fact]
            public void Should_throw_an_exception_if_you_try_to_get_the_field_count()
            {
                int dummy;
                Assert.Throws<InvalidOperationException>(() => dummy = this.reader.FieldCount);
            }

            [Fact]
            public void Should_not_throw_an_exception_if_you_try_to_get_the_number_of_records_affected()
            {
                int dummy = this.reader.RecordsAffected;
            }

            [Fact]
            public void Should_throw_an_exception_if_you_try_to_get_the_depth()
            {
                int dummy;
                Assert.Throws<InvalidOperationException>(() => dummy = this.reader.Depth);
            }

            [Fact]
            public void Should_throw_an_exception_if_you_try_to_read()
            {
                Assert.Throws<InvalidOperationException>(() => this.reader.Read());
            }

            [Fact]
            public void Should_throw_an_exception_if_you_try_to_move_to_the_next_result()
            {
                Assert.Throws<InvalidOperationException>(() => this.reader.NextResult());
            }

            [Fact]
            public void Should_throw_an_exception_if_you_try_to_move_to_get_the_row()
            {
                var dummy = new object[10];
                Assert.Throws<InvalidOperationException>(() => this.reader.GetValues(dummy));
            }

            [Fact]
            public void Should_throw_an_exception_if_you_try_to_get_a_column_name()
            {
                Assert.Throws<InvalidOperationException>(() => this.reader.GetName(0));
            }

            [Fact]
            public void Should_throw_an_exception_if_you_try_to_get_a_columns_ordinal_position()
            {
                Assert.Throws<InvalidOperationException>(() => this.reader.GetOrdinal("foo"));
            }

            [Fact]
            public void Should_throw_an_exception_if_you_try_to_get_a_columns_value()
            {
                Assert.Throws<InvalidOperationException>(() => this.reader.GetValue(0));
            }

            [Fact]
            public void Should_throw_an_exception_if_you_try_to_get_a_columns_type()
            {
                Assert.Throws<InvalidOperationException>(() => this.reader.GetFieldType(0));
            }

            [Fact]
            public void Should_throw_an_exception_if_you_try_to_get_a_columns_data_type_name()
            {
                Assert.Throws<InvalidOperationException>(() => this.reader.GetDataTypeName(0));
            }

            [Fact]
            public void Should_clean_underlaying_stream()
            {
                var field = this.reader.GetType().GetField("stream", BindingFlags.NonPublic | BindingFlags.Instance);
                Assert.NotNull(field);

                var streamValue = field.GetValue(this.reader);
                Assert.Null(streamValue);
            }

            [Fact]
            public void Should_clean_underlaying_reader()
            {
                var field = this.reader.GetType().GetField("reader", BindingFlags.NonPublic | BindingFlags.Instance);
                Assert.NotNull(field);

                var readerValue = field.GetValue(this.reader);
                Assert.Null(readerValue);
            }
        }

        public class When_the_reader_has_been_disposed
        {
            private WeakReference streamRef;
            private WeakReference readerRef;

            public When_the_reader_has_been_disposed()
            {
                var stream = new MemoryStream();
                using (var table = TestData.SmallDataTable())
                using (var tableReader = table.CreateDataReader())
                {
                    DataSerializer.Serialize(stream, tableReader);
                }

                stream.Seek(0, SeekOrigin.Begin);
                var reader = DataSerializer.Deserialize(stream);

                this.streamRef = new WeakReference(stream);
                this.readerRef = new WeakReference(reader);

                reader.Dispose();
            }

            [Fact]
            public void Reader_should_collected_by_GC()
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();

                Assert.False(this.readerRef.IsAlive);
                Assert.False(this.streamRef.IsAlive);
            }
        }

        public class When_the_proto_stream_has_been_closed
        {
            private ProtoDataStream protoStream;

            public When_the_proto_stream_has_been_closed()
            {
                using (var table = TestData.SmallDataTable())
                using (var tableReader = table.CreateDataReader())
                {
                    this.protoStream = new ProtoDataStream(tableReader);
                }

                this.protoStream.Close();
            }

            [Fact]
            public void CanRead_should_be_false()
            {
                Assert.False(this.protoStream.CanRead);
            }

            [Fact]
            public void Should_not_throw_an_exception_if_you_try_to_close_it_twice()
            {
                this.protoStream.Close();
                this.protoStream.Close();
            }

            [Fact]
            public void Position_should_throw_exception()
            {
                Assert.Throws<InvalidOperationException>(() =>
                {
                    var p = this.protoStream.Position;
                });
            }

            [Fact]
            public void Should_clean_underlaying_stream()
            {
                var field = this.protoStream.GetType().GetField("bufferStream", BindingFlags.NonPublic | BindingFlags.Instance);
                Assert.NotNull(field);

                var streamValue = field.GetValue(this.protoStream);
                Assert.Null(streamValue);
            }

            [Fact]
            public void Should_clean_underlaying_reader()
            {
                var field = this.protoStream.GetType().GetField("reader", BindingFlags.NonPublic | BindingFlags.Instance);
                Assert.NotNull(field);

                var readerValue = field.GetValue(this.protoStream);
                Assert.Null(readerValue);
            }

            [Fact]
            public void Should_clean_underlaying_writer()
            {
                var field = this.protoStream.GetType().GetField("writer", BindingFlags.NonPublic | BindingFlags.Instance);
                Assert.NotNull(field);

                var writerValue = field.GetValue(this.protoStream);
                Assert.Null(writerValue);
            }
        }

        public class When_the_proto_stream_has_been_disposed
        {
            private WeakReference weakRef;

            public When_the_proto_stream_has_been_disposed()
            {
                Stream stream;
                using (var table = TestData.SmallDataTable())
                using (var tableReader = table.CreateDataReader())
                {
                    stream = new ProtoDataStream(tableReader);
                }

                Assert.True(stream.CanRead);

                this.weakRef = new WeakReference(stream);

                stream.Dispose();
            }

            [Fact]
            public void Proto_stream_should_collected_by_GC()
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();

                Assert.False(this.weakRef.IsAlive);
            }
        }
    }
}