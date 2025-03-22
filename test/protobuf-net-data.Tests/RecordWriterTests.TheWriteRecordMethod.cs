// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

#pragma warning disable CS0618
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class RecordWriterTests
    {
        public class TheWriteRecordMethod : RecordWriterTests
        {
            [Fact]
            public void ShouldSerializeStringValue()
            {
                // Arrange
                var value = "foo";
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilFieldValue();

                Assert.Equal(value, reader.ReadString());
            }

            [Fact]
            public void ShouldSerializeStringColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader("foo");

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilColumnType();

                Assert.Equal(1, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeDateTimeValue()
            {
                // Arrange
                var value = new DateTime(1969, 10, 29, 22, 30, 0);
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilFieldValue();

                Assert.Equal(value, BclHelpers.ReadDateTime(reader));
            }

            [Fact]
            public void ShouldSerializeDateTimeColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(new DateTime(1969, 10, 29, 22, 30, 0));

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilColumnType();

                Assert.Equal(2, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeInt32Value()
            {
                // Arrange
                var value = 42;
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilFieldValue();

                Assert.Equal(value, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeInt32ColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(42);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilColumnType();

                Assert.Equal(3, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeInt64Value()
            {
                // Arrange
                var value = 42L;
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilFieldValue();

                Assert.Equal(value, reader.ReadInt64());
            }

            [Fact]
            public void ShouldSerializeInt64ColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(42L);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilColumnType();

                Assert.Equal(4, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeInt16Value()
            {
                // Arrange
                var value = (short)42;
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilFieldValue();

                Assert.Equal(value, reader.ReadInt16());
            }

            [Fact]
            public void ShouldSerializeInt16ColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader((short)42);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilColumnType();

                Assert.Equal(5, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeBooleanValue()
            {
                // Arrange
                var value = true;
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilFieldValue();

                Assert.Equal(value, reader.ReadBoolean());
            }

            [Fact]
            public void ShouldSerializeBooleanColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(true);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilColumnType();

                Assert.Equal(6, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeByteValue()
            {
                // Arrange
                var value = (byte)42;
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilFieldValue();

                Assert.Equal(value, reader.ReadByte());
            }

            [Fact]
            public void ShouldSerializeByteColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader((byte)42);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilColumnType();

                Assert.Equal(7, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeFloatValue()
            {
                // Arrange
                var value = 42f;
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilFieldValue();

                Assert.Equal(value, reader.ReadSingle());
            }

            [Fact]
            public void ShouldSerializeFloatColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(42f);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilColumnType();

                Assert.Equal(8, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeDoubleValue()
            {
                // Arrange
                var value = 42d;
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilFieldValue();

                Assert.Equal(value, reader.ReadDouble());
            }

            [Fact]
            public void ShouldSerializeDoubleColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(42d);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilColumnType();

                Assert.Equal(9, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeGuidValue()
            {
                // Arrange
                var value = Guid.NewGuid();
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilFieldValue();

                Assert.Equal(value, BclHelpers.ReadGuid(reader));
            }

            [Fact]
            public void ShouldSerializeGuidColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(Guid.NewGuid());

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilColumnType();

                Assert.Equal(10, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeCharValue()
            {
                // Arrange
                var value = ';';
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilFieldValue();

                Assert.Equal(value, (char)reader.ReadInt16());
            }

            [Fact]
            public void ShouldSerializeCharColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(';');

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilColumnType();

                Assert.Equal(11, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeDecimalValue()
            {
                // Arrange
                var value = 42m;
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilFieldValue();

                Assert.Equal(value, BclHelpers.ReadDecimal(reader));
            }

            [Fact]
            public void ShouldSerializeDecimalColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(42m);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilColumnType();

                Assert.Equal(12, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeByteArrayValue()
            {
                // Arrange
                var value = new[] { (byte)42, (byte)42 };
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilFieldValue();

                Assert.Equal(value, ProtoReader.AppendBytes(null, reader));
            }

            [Fact]
            public void ShouldSerializeEmptyByteArrayAsNull()
            {
                // Arrange
                var dataReader = this.CreateDataReader(new byte[0]);
                var options = new ProtoDataWriterOptions() { SerializeEmptyArraysAsNull = true };

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader, options), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilField();

                readerContext.ReadExpectedFieldHeader(3);
                readerContext.StartSubItem();

                Assert.Equal(0, reader.ReadFieldHeader());
            }

            [Fact]
            public void ShouldSerializeByteArrayColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(new[] { (byte)42, (byte)42 });

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilColumnType();

                Assert.Equal(13, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeCharArrayValue()
            {
                // Arrange
                var value = new[] { 'f', 'o', 'o' };
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilFieldValue();

                Assert.Equal(value, reader.ReadString().ToCharArray());
            }

            [Fact]
            public void ShouldSerializeEmptyCharArrayAsNull()
            {
                // Arrange
                var dataReader = this.CreateDataReader(new char[0]);
                var options = new ProtoDataWriterOptions() { SerializeEmptyArraysAsNull = true };

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader, options), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilField();

                readerContext.ReadExpectedFieldHeader(3);
                readerContext.StartSubItem();

                Assert.Equal(0, reader.ReadFieldHeader());
            }

            [Fact]
            public void ShouldSerializeCharArrayColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(new[] { 'f', 'o', 'o' });

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilColumnType();

                Assert.Equal(14, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeTimeSpanValue()
            {
                // Arrange
                var value = TimeSpan.FromTicks(1);
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilFieldValue();

                Assert.Equal(value, BclHelpers.ReadTimeSpan(reader));
            }

            [Fact]
            public void ShouldSerializeTimeSpanColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(TimeSpan.FromTicks(1));

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilColumnType();

                Assert.Equal(15, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeDateTimeOffsetValue()
            {
                // Arrange
                var value = DateTimeOffset.FromUnixTimeSeconds(1);
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilFieldValue();

                Assert.Equal(value, DateTimeOffset.Parse(reader.ReadString()));
            }

            [Fact]
            public void ShouldSerializeDateTimeOffsetColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(DateTimeOffset.FromUnixTimeSeconds(1));

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilColumnType();

                Assert.Equal(16, reader.ReadInt32());
            }

            [Fact]
            public void ShouldNotSerializeIfValueIsNull()
            {
                // Arrange
                string value = null;
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilField();
                readerContext.ReadExpectedFieldHeader(3);

                Assert.Equal(0, reader.ReadFieldHeader());
            }

            [Fact]
            public void ShouldNotSerializeIfValueIsDBNull()
            {
                // Arrange
                var value = DBNull.Value;
                var dataTable = new DataTable();

                dataTable.Columns.Add("foo", typeof(string));

                dataTable.Rows.Add(value);

                var dataReader = dataTable.CreateDataReader();

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilField();
                readerContext.ReadExpectedFieldHeader(3);

                Assert.Equal(0, reader.ReadFieldHeader());
            }

            [Fact]
            public void ShouldNotSerializeIfValueIsEmptyArray()
            {
                // Arrange
                var value = new char[0];
                var dataReader = this.CreateDataReader(value);

                var options = new ProtoDataWriterOptions() { SerializeEmptyArraysAsNull = true };

                // Act
                var reader = ProtoReader.Create(this.Serialize(dataReader, options), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilField();
                readerContext.ReadExpectedFieldHeader(3);

                Assert.Equal(0, reader.ReadFieldHeader());
            }

            private IDataReader CreateDataReader<TDataType>(TDataType value)
            {
                var dataTable = new DataTable();

                dataTable.Columns.Add(typeof(TDataType).Name, typeof(TDataType));

                dataTable.Rows.Add(value);

                return dataTable.CreateDataReader();
            }

            private Stream Serialize(IDataReader dataReader, ProtoDataWriterOptions options = null)
            {
                var writer = new ProtoDataWriter();
                var stream = new MemoryStream();

                writer.Serialize(stream, dataReader, options);

                stream.Position = 0;

                return stream;
            }
        }
    }
}
#pragma warning restore CS0618
