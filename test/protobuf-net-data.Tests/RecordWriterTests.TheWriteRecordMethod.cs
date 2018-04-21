// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class RecordWriterTests
    {
        private const int ResultFieldHeader = 1;
        private const int ColumnFieldHeader = 2;
        private const int ColumnNameFieldHeader = 1;
        private const int ColumnTypeFieldHeader = 2;
        private const int NoneFieldHeader = 0;
        private const int RecordFieldHeader = 3;

        private readonly Stack<SubItemToken> tokens = new Stack<SubItemToken>();

        public class TheWriteRecordMethod : RecordWriterTests
        {
            [Fact]
            public void ShouldSerializeStringValue()
            {
                // Arrange
                var value = "foo";
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilFieldValue(reader);

                Assert.Equal(value, reader.ReadString());
            }

            [Fact]
            public void ShouldSerializeStringColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader("foo");

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilColumnType(reader);

                Assert.Equal(1, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeDateTimeValue()
            {
                // Arrange
                var value = new DateTime(1969, 10, 29, 22, 30, 0);
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilFieldValue(reader);

                Assert.Equal(value, BclHelpers.ReadDateTime(reader));
            }

            [Fact]
            public void ShouldSerializeDateTimeColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(new DateTime(1969, 10, 29, 22, 30, 0));

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilColumnType(reader);

                Assert.Equal(2, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeInt32Value()
            {
                // Arrange
                var value = 42;
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilFieldValue(reader);

                Assert.Equal(value, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeInt32ColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(42);

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilColumnType(reader);

                Assert.Equal(3, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeInt64Value()
            {
                // Arrange
                var value = 42L;
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilFieldValue(reader);

                Assert.Equal(value, reader.ReadInt64());
            }

            [Fact]
            public void ShouldSerializeInt64ColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(42L);

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilColumnType(reader);

                Assert.Equal(4, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeInt16Value()
            {
                // Arrange
                var value = (short)42;
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilFieldValue(reader);

                Assert.Equal(value, reader.ReadInt16());
            }

            [Fact]
            public void ShouldSerializeInt16ColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader((short)42);

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilColumnType(reader);

                Assert.Equal(5, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeBooleanValue()
            {
                // Arrange
                var value = true;
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilFieldValue(reader);

                Assert.Equal(value, reader.ReadBoolean());
            }

            [Fact]
            public void ShouldSerializeBooleanColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(true);

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilColumnType(reader);

                Assert.Equal(6, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeByteValue()
            {
                // Arrange
                var value = (byte)42;
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilFieldValue(reader);

                Assert.Equal(value, reader.ReadByte());
            }

            [Fact]
            public void ShouldSerializeByteColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader((byte)42);

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilColumnType(reader);

                Assert.Equal(7, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeFloatValue()
            {
                // Arrange
                var value = 42f;
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilFieldValue(reader);

                Assert.Equal(value, reader.ReadSingle());
            }

            [Fact]
            public void ShouldSerializeFloatColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(42f);

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilColumnType(reader);

                Assert.Equal(8, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeDoubleValue()
            {
                // Arrange
                var value = 42d;
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilFieldValue(reader);

                Assert.Equal(value, reader.ReadDouble());
            }

            [Fact]
            public void ShouldSerializeDoubleColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(42d);

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilColumnType(reader);

                Assert.Equal(9, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeGuidValue()
            {
                // Arrange
                var value = Guid.NewGuid();
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilFieldValue(reader);

                Assert.Equal(value, BclHelpers.ReadGuid(reader));
            }

            [Fact]
            public void ShouldSerializeGuidColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(Guid.NewGuid());

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilColumnType(reader);

                Assert.Equal(10, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeCharValue()
            {
                // Arrange
                var value = ';';
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilFieldValue(reader);

                Assert.Equal(value, (char)reader.ReadInt16());
            }

            [Fact]
            public void ShouldSerializeCharColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(';');

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilColumnType(reader);

                Assert.Equal(11, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeDecimalValue()
            {
                // Arrange
                var value = 42m;
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilFieldValue(reader);

                Assert.Equal(value, BclHelpers.ReadDecimal(reader));
            }

            [Fact]
            public void ShouldSerializeDecimalColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(42m);

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilColumnType(reader);

                Assert.Equal(12, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeByteArrayValue()
            {
                // Arrange
                var value = new[] { (byte)42, (byte)42 };
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilFieldValue(reader);

                Assert.Equal(value, ProtoReader.AppendBytes(null, reader));
            }

            [Fact]
            public void ShouldSerializeByteArrayColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(new[] { (byte)42, (byte)42 });

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilColumnType(reader);

                Assert.Equal(13, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeCharArrayValue()
            {
                // Arrange
                var value = new[] { 'f', 'o', 'o' };
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilFieldValue(reader);

                Assert.Equal(value, reader.ReadString().ToCharArray());
            }

            [Fact]
            public void ShouldSerializeCharArrayColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(new[] { 'f', 'o', 'o' });

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilColumnType(reader);

                Assert.Equal(14, reader.ReadInt32());
            }

            [Fact]
            public void ShouldSerializeTimeSpanValue()
            {
                // Arrange
                var value = TimeSpan.FromTicks(1);
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilFieldValue(reader);

                Assert.Equal(value, BclHelpers.ReadTimeSpan(reader));
            }

            [Fact]
            public void ShouldSerializeTimeSpanColumnType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(TimeSpan.FromTicks(1));

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilColumnType(reader);

                Assert.Equal(15, reader.ReadInt32());
            }

            [Fact]
            public void ShouldNotSerializeIfValueIsNull()
            {
                // Arrange
                string value = null;
                var dataReader = this.CreateDataReader(value);

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilField(reader);
                this.ReadExpectedFieldHeader(reader, 3);

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
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                this.ReadUntilField(reader);
                this.ReadExpectedFieldHeader(reader, 3);

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
                var reader = new ProtoReader(this.Serialize(dataReader, options), null, null);

                // Assert
                this.ReadUntilField(reader);
                this.ReadExpectedFieldHeader(reader, 3);

                Assert.Equal(0, reader.ReadFieldHeader());
            }

            private Stream Serialize(IDataReader dataReader, ProtoDataWriterOptions options = null)
            {
                var writer = new ProtoDataWriter();
                var stream = new MemoryStream();

                writer.Serialize(stream, dataReader, options);

                stream.Position = 0;

                return stream;
            }

            private IDataReader CreateDataReader<TDataType>(TDataType value)
            {
                var dataTable = new DataTable();

                dataTable.Columns.Add(typeof(TDataType).Name, typeof(TDataType));

                dataTable.Rows.Add(value);

                return dataTable.CreateDataReader();
            }

            private void ReadUntilField(ProtoReader reader)
            {
                this.ReadUntilColumnType(reader);

                reader.ReadInt32();

                this.ReadExpectedFieldHeader(reader, NoneFieldHeader);
                this.EndSubItem(reader);
            }

            private void ReadUntilFieldValue(ProtoReader reader)
            {
                this.ReadUntilField(reader);

                this.ReadExpectedFieldHeader(reader, RecordFieldHeader);
                this.StartSubItem(reader);
                this.ReadExpectedFieldHeader(reader, 1);
            }

            private void ReadUntilColumnType(ProtoReader reader)
            {
                this.ReadExpectedFieldHeader(reader, ResultFieldHeader);
                this.StartSubItem(reader);

                this.ReadExpectedFieldHeader(reader, ColumnFieldHeader);
                this.StartSubItem(reader);
                this.ReadExpectedFieldHeader(reader, ColumnNameFieldHeader);

                reader.ReadString();

                this.ReadExpectedFieldHeader(reader, ColumnTypeFieldHeader);
            }

            private void ReadExpectedFieldHeader(ProtoReader reader, int expectedFieldHeader)
            {
                var fieldHeader = reader.ReadFieldHeader();

                if (fieldHeader != expectedFieldHeader)
                {
                    throw new InvalidDataException($"Field header {expectedFieldHeader} expected, actual '{fieldHeader}'.");
                }
            }

            private void StartSubItem(ProtoReader reader)
            {
                this.tokens.Push(ProtoReader.StartSubItem(reader));
            }

            private void EndSubItem(ProtoReader reader)
            {
                ProtoReader.EndSubItem(this.tokens.Pop(), reader);
            }
        }
    }
}
