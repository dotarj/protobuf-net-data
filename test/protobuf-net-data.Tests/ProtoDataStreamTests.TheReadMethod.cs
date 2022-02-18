// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

#pragma warning disable CS0618
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataStreamTests
    {
        public class TheReadMethod : ProtoDataStreamTests
        {
            private const int ResultFieldHeader = 1;

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

                var reader = ProtoReader.Create(outputStream, null, null);

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
                var reader = ProtoReader.Create(this.CopyStream(stream), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilColumnName();

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
                var reader = ProtoReader.Create(this.CopyStream(stream), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilFieldValue();

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
                var reader = ProtoReader.Create(this.CopyStream(stream), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilResultEnd();

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
                var reader = ProtoReader.Create(this.CopyStream(stream), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilResultEnd();
                readerContext.ReadUntilColumnName();

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
        }
    }
}
#pragma warning restore CS0618
