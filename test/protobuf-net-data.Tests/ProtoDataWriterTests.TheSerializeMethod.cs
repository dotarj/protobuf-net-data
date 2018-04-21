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

        private readonly Stack<SubItemToken> tokens = new Stack<SubItemToken>();

        [Fact]
        public void ShouldThrowIfStreamIsNull()
        {
            // Arrange
            var writer = new ProtoDataWriter();
            var dataReader = this.CreateDataReader(1);

            // Assert
            Assert.Throws<ArgumentNullException>(() => writer.Serialize(null, dataReader));
        }

        [Fact]
        public void ShouldThrowIfReaderIsNull()
        {
            // Arrange
            var writer = new ProtoDataWriter();
            var stream = new MemoryStream();

            // Assert
            Assert.Throws<ArgumentNullException>(() => writer.Serialize(stream, (IDataReader)null));
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

            // Act
            var reader = new ProtoReader(this.Serialize(dataReader), null, null);

            // Assert
            var readerContext = new ProtoReaderContext(reader);

            readerContext.ReadUntilResultEnd();

            Assert.Equal(ResultFieldHeader, reader.ReadFieldHeader());
        }

        private Stream Serialize(IDataReader dataReader, ProtoDataWriterOptions options = null)
        {
            var writer = new ProtoDataWriter();
            var stream = new MemoryStream();

            writer.Serialize(stream, dataReader);

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
    }
}
