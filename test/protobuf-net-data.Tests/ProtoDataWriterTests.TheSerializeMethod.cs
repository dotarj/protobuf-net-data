// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataWriterTests
    {
        private const int ResultFieldHeader = 1;
        private const int ColumnFieldHeader = 2;
        private const int ColumnNameFieldHeader = 1;
        private const int ColumnTypeFieldHeader = 2;
        private const int NoneFieldHeader = 0;
        private const int RecordFieldHeader = 3;

        private readonly Stack<SubItemToken> tokens = new Stack<SubItemToken>();

        [Fact]
        public void ShouldThrowIfStreamIsNull()
        {
            // Arrange
            var dataReader = DataReaderHelper.CreateDataReader(1);

            // Assert
            Assert.Throws<ArgumentNullException>(() => this.writer.Serialize(null, dataReader));
        }

        [Fact]
        public void ShouldThrowIfReaderIsNull()
        {
            // Assert
            Assert.Throws<ArgumentNullException>(() => this.writer.Serialize(this.stream, (IDataReader)null));
        }

        [Fact]
        public void ShouldSerializeStringValue()
        {
            // Arrange
            var value = "foo";
            var dataReader = DataReaderHelper.CreateDataReader(value);

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilFieldValue();

            Assert.Equal(value, this.reader.ReadString());
        }

        [Fact]
        public void ShouldSerializeStringColumnType()
        {
            // Arrange
            var dataReader = DataReaderHelper.CreateDataReader("foo");

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilColumnType();

            Assert.Equal(1, this.reader.ReadInt32());
        }

        [Fact]
        public void ShouldSerializeDateTimeValue()
        {
            // Arrange
            var value = new DateTime(1969, 10, 29, 22, 30, 0);
            var dataReader = DataReaderHelper.CreateDataReader(value);

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilFieldValue();

            Assert.Equal(value, BclHelpers.ReadDateTime(this.reader));
        }

        [Fact]
        public void ShouldSerializeDateTimeColumnType()
        {
            // Arrange
            var dataReader = DataReaderHelper.CreateDataReader(new DateTime(1969, 10, 29, 22, 30, 0));

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilColumnType();

            Assert.Equal(2, this.reader.ReadInt32());
        }

        [Fact]
        public void ShouldSerializeInt32Value()
        {
            // Arrange
            var value = 42;
            var dataReader = DataReaderHelper.CreateDataReader(value);

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilFieldValue();

            Assert.Equal(value, this.reader.ReadInt32());
        }

        [Fact]
        public void ShouldSerializeInt32ColumnType()
        {
            // Arrange
            var dataReader = DataReaderHelper.CreateDataReader(42);

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilColumnType();

            Assert.Equal(3, this.reader.ReadInt32());
        }

        [Fact]
        public void ShouldSerializeInt64Value()
        {
            // Arrange
            var value = 42L;
            var dataReader = DataReaderHelper.CreateDataReader(value);

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilFieldValue();

            Assert.Equal(value, this.reader.ReadInt64());
        }

        [Fact]
        public void ShouldSerializeInt64ColumnType()
        {
            // Arrange
            var dataReader = DataReaderHelper.CreateDataReader(42L);

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilColumnType();

            Assert.Equal(4, this.reader.ReadInt32());
        }

        [Fact]
        public void ShouldSerializeInt16Value()
        {
            // Arrange
            var value = (short)42;
            var dataReader = DataReaderHelper.CreateDataReader(value);

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilFieldValue();

            Assert.Equal(value, this.reader.ReadInt16());
        }

        [Fact]
        public void ShouldSerializeInt16ColumnType()
        {
            // Arrange
            var dataReader = DataReaderHelper.CreateDataReader((short)42);

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilColumnType();

            Assert.Equal(5, this.reader.ReadInt32());
        }

        [Fact]
        public void ShouldSerializeBooleanValue()
        {
            // Arrange
            var value = true;
            var dataReader = DataReaderHelper.CreateDataReader(value);

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilFieldValue();

            Assert.Equal(value, this.reader.ReadBoolean());
        }

        [Fact]
        public void ShouldSerializeBooleanColumnType()
        {
            // Arrange
            var dataReader = DataReaderHelper.CreateDataReader(true);

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilColumnType();

            Assert.Equal(6, this.reader.ReadInt32());
        }

        [Fact]
        public void ShouldSerializeByteValue()
        {
            // Arrange
            var value = (byte)42;
            var dataReader = DataReaderHelper.CreateDataReader(value);

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilFieldValue();

            Assert.Equal(value, this.reader.ReadByte());
        }

        [Fact]
        public void ShouldSerializeByteColumnType()
        {
            // Arrange
            var dataReader = DataReaderHelper.CreateDataReader((byte)42);

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilColumnType();

            Assert.Equal(7, this.reader.ReadInt32());
        }

        [Fact]
        public void ShouldSerializeFloatValue()
        {
            // Arrange
            var value = 42f;
            var dataReader = DataReaderHelper.CreateDataReader(value);

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilFieldValue();

            Assert.Equal(value, this.reader.ReadSingle());
        }

        [Fact]
        public void ShouldSerializeFloatColumnType()
        {
            // Arrange
            var dataReader = DataReaderHelper.CreateDataReader(42f);

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilColumnType();

            Assert.Equal(8, this.reader.ReadInt32());
        }

        [Fact]
        public void ShouldSerializeDoubleValue()
        {
            // Arrange
            var value = 42d;
            var dataReader = DataReaderHelper.CreateDataReader(value);

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilFieldValue();

            Assert.Equal(value, this.reader.ReadDouble());
        }

        [Fact]
        public void ShouldSerializeDoubleColumnType()
        {
            // Arrange
            var dataReader = DataReaderHelper.CreateDataReader(42d);

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilColumnType();

            Assert.Equal(9, this.reader.ReadInt32());
        }

        [Fact]
        public void ShouldSerializeGuidValue()
        {
            // Arrange
            var value = Guid.NewGuid();
            var dataReader = DataReaderHelper.CreateDataReader(value);

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilFieldValue();

            Assert.Equal(value, BclHelpers.ReadGuid(this.reader));
        }

        [Fact]
        public void ShouldSerializeGuidColumnType()
        {
            // Arrange
            var dataReader = DataReaderHelper.CreateDataReader(Guid.NewGuid());

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilColumnType();

            Assert.Equal(10, this.reader.ReadInt32());
        }

        [Fact]
        public void ShouldSerializeCharValue()
        {
            // Arrange
            var value = ';';
            var dataReader = DataReaderHelper.CreateDataReader(value);

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilFieldValue();

            Assert.Equal(value, (char)this.reader.ReadInt16());
        }

        [Fact]
        public void ShouldSerializeCharColumnType()
        {
            // Arrange
            var dataReader = DataReaderHelper.CreateDataReader(';');

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilColumnType();

            Assert.Equal(11, this.reader.ReadInt32());
        }

        [Fact]
        public void ShouldSerializeDecimalValue()
        {
            // Arrange
            var value = 42m;
            var dataReader = DataReaderHelper.CreateDataReader(value);

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilFieldValue();

            Assert.Equal(value, BclHelpers.ReadDecimal(this.reader));
        }

        [Fact]
        public void ShouldSerializeDecimalColumnType()
        {
            // Arrange
            var dataReader = DataReaderHelper.CreateDataReader(42m);

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilColumnType();

            Assert.Equal(12, this.reader.ReadInt32());
        }

        [Fact]
        public void ShouldSerializeByteArrayValue()
        {
            // Arrange
            var value = new[] { (byte)42, (byte)42 };
            var dataReader = DataReaderHelper.CreateDataReader(value);

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilFieldValue();

            Assert.Equal(value, ProtoReader.AppendBytes(null, this.reader));
        }

        [Fact]
        public void ShouldSerializeByteArrayColumnType()
        {
            // Arrange
            var dataReader = DataReaderHelper.CreateDataReader(new[] { (byte)42, (byte)42 });

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilColumnType();

            Assert.Equal(13, this.reader.ReadInt32());
        }

        [Fact]
        public void ShouldSerializeCharArrayValue()
        {
            // Arrange
            var value = new[] { 'f', 'o', 'o' };
            var dataReader = DataReaderHelper.CreateDataReader(value);

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilFieldValue();

            Assert.Equal(value, this.reader.ReadString().ToCharArray());
        }

        [Fact]
        public void ShouldSerializeCharArrayColumnType()
        {
            // Arrange
            var dataReader = DataReaderHelper.CreateDataReader(new[] { 'f', 'o', 'o' });

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilColumnType();

            Assert.Equal(14, this.reader.ReadInt32());
        }

        [Fact]
        public void ShouldSerializeTimeSpanValue()
        {
            // Arrange
            var value = TimeSpan.FromTicks(1);
            var dataReader = DataReaderHelper.CreateDataReader(value);

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilFieldValue();

            Assert.Equal(value, BclHelpers.ReadTimeSpan(this.reader));
        }

        [Fact]
        public void ShouldSerializeTimeSpanColumnType()
        {
            // Arrange
            var dataReader = DataReaderHelper.CreateDataReader(TimeSpan.FromTicks(1));

            // Act
            this.writer.Serialize(this.stream, dataReader);

            // Assert
            this.stream.Position = 0;

            this.ReadUntilColumnType();

            Assert.Equal(15, this.reader.ReadInt32());
        }

        private void ReadUntilFieldValue()
        {
            this.ReadUntilColumnType();

            this.reader.ReadInt32();

            this.ReadExpectedFieldHeader(NoneFieldHeader);
            this.EndSubItem();

            this.ReadExpectedFieldHeader(RecordFieldHeader);
            this.StartSubItem();
            this.ReadExpectedFieldHeader(1);
        }

        private void ReadUntilColumnType()
        {
            this.ReadExpectedFieldHeader(ResultFieldHeader);
            this.StartSubItem();

            this.ReadExpectedFieldHeader(ColumnFieldHeader);
            this.StartSubItem();
            this.ReadExpectedFieldHeader(ColumnNameFieldHeader);
            this.reader.ReadString();
            this.ReadExpectedFieldHeader(ColumnTypeFieldHeader);
        }

        private void ReadExpectedFieldHeader(int expectedFieldHeader)
        {
            var fieldHeader = this.reader.ReadFieldHeader();

            if (fieldHeader != expectedFieldHeader)
            {
                throw new InvalidDataException($"Field header {expectedFieldHeader} expected, actual '{fieldHeader}'.");
            }
        }

        private void StartSubItem()
        {
            this.tokens.Push(ProtoReader.StartSubItem(this.reader));
        }

        private void EndSubItem()
        {
            ProtoReader.EndSubItem(this.tokens.Pop(), this.reader);
        }
    }
}
