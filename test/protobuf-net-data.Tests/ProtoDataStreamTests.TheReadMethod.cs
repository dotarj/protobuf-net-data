// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataStreamTests
    {
        private const int ResultFieldHeader = 1;
        private const int ColumnFieldHeader = 2;
        private const int ColumnNameFieldHeader = 1;
        private const int ColumnTypeFieldHeader = 2;
        private const int NoneFieldHeader = 0;
        private const int RecordFieldHeader = 3;

        private readonly Stack<SubItemToken> tokens = new Stack<SubItemToken>();

        public class TheReadMethod : ProtoDataStreamTests
        {
            [Fact]
            public void ShouldReturnZeroWhenReaderIsClosed()
            {
                // Arrange
                var stream = new ProtoDataStream(this.CreateDataReader("foo"));

                stream.Read(new byte[1024], 0, 1024);

                // Act
                var result = stream.Read(new byte[1024], 0, 1024);

                // Assert
                Assert.Equal(0, result);
            }

            [Fact]
            public void ShouldSerializeUsingMultipleIterations()
            {
                // Arrange
                var stream = new ProtoDataStream(this.CreateDataReader("foo", 1000));
                var outputStream = new MemoryStream();

                var buffer = new byte[1];
                int read;

                // Act
                while ((read = stream.Read(buffer, 0, buffer.Length)) > 0)
                {
                    outputStream.Write(buffer, 0, read);
                }

                // Assert
                outputStream.Position = 0;

                var reader = new ProtoReader(outputStream, null, null);

                Assert.Equal(ResultFieldHeader, reader.ReadFieldHeader());
            }

            [Fact]
            public void ShouldSerializeColumns()
            {
                // Arrange
                var columnName = "foo";
                var dataTable = new DataTable();

                dataTable.Columns.Add(columnName, typeof(int));

                dataTable.Rows.Add(1);

                var dataReader = dataTable.CreateDataReader();
                var stream = new ProtoDataStream(dataReader);

                // Act
                var reader = new ProtoReader(this.CopyStream(stream), null, null);

                // Assert
                this.ReadUntilColumnName(reader);

                Assert.Equal(columnName, reader.ReadString());
            }

            [Fact]
            public void ShouldSerializeField()
            {
                // Arrange
                var value = "foo";
                var dataReader = this.CreateDataReader(value);

                var stream = new ProtoDataStream(dataReader);

                // Act
                var reader = new ProtoReader(this.CopyStream(stream), null, null);

                // Assert
                this.ReadUntilFieldValue(reader);

                Assert.Equal(value, reader.ReadString());
            }

            [Fact]
            public void ShouldSerializeMultipleResults()
            {
                // Arrange
                var dataSet = new DataSet();

                dataSet.Tables.Add(new DataTable());
                dataSet.Tables.Add(new DataTable());

                dataSet.Tables[0].Columns.Add("foo", typeof(int));
                dataSet.Tables[0].Rows.Add(1);

                dataSet.Tables[1].Columns.Add("bar", typeof(int));
                dataSet.Tables[1].Rows.Add(1);

                var dataReader = dataSet.CreateDataReader();

                var stream = new ProtoDataStream(dataReader);

                // Act
                var reader = new ProtoReader(this.CopyStream(stream), null, null);

                // Assert
                this.ReadUntilResultEnd(reader);

                Assert.Equal(ResultFieldHeader, reader.ReadFieldHeader());
            }

            [Fact]
            public void ShouldSerializeSuccessiveResultColumns()
            {
                // Arrange
                var dataSet = new DataSet();

                dataSet.Tables.Add(new DataTable());
                dataSet.Tables.Add(new DataTable());

                dataSet.Tables[0].Columns.Add("foo", typeof(int));
                dataSet.Tables[0].Rows.Add(1);

                var columnName = "bar";

                dataSet.Tables[1].Columns.Add(columnName, typeof(int));
                dataSet.Tables[1].Rows.Add(1);

                var dataReader = dataSet.CreateDataReader();

                var stream = new ProtoDataStream(dataReader);

                // Act
                var reader = new ProtoReader(this.CopyStream(stream), null, null);

                // Assert
                this.ReadUntilResultEnd(reader);
                this.ReadUntilColumnName(reader);

                Assert.Equal(columnName, reader.ReadString());
            }

            [Fact]
            public void ShouldDisposeReader()
            {
                // Arrange
                var reader = this.CreateDataReader("foo");
                var stream = new ProtoDataStream(reader);

                // Act
                stream.Read(new byte[1024], 0, 1024);

                // Assert
                Assert.Throws<InvalidOperationException>(() => reader.FieldCount);
            }

            private Stream CopyStream(Stream inputStream)
            {
                var outputStream = new MemoryStream();

                var buffer = new byte[1024];
                int read;

                while ((read = inputStream.Read(buffer, 0, buffer.Length)) > 0)
                {
                    outputStream.Write(buffer, 0, read);
                }

                outputStream.Position = 0;

                return outputStream;
            }

            private void ReadUntilResultEnd(ProtoReader reader)
            {
                this.ReadUntilFieldValue(reader);

                reader.ReadInt32();

                this.ReadExpectedFieldHeader(reader, NoneFieldHeader);
                this.EndSubItem(reader);
                this.ReadExpectedFieldHeader(reader, NoneFieldHeader);
                this.EndSubItem(reader);
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
                this.ReadUntilColumnName(reader);

                reader.ReadString();

                this.ReadExpectedFieldHeader(reader, ColumnTypeFieldHeader);
            }

            private void ReadUntilColumnName(ProtoReader reader)
            {
                this.ReadExpectedFieldHeader(reader, ResultFieldHeader);
                this.StartSubItem(reader);

                this.ReadExpectedFieldHeader(reader, ColumnFieldHeader);
                this.StartSubItem(reader);
                this.ReadExpectedFieldHeader(reader, ColumnNameFieldHeader);
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
