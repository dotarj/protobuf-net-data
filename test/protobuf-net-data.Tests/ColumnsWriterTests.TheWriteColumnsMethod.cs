// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ColumnsWriterTests
    {
        private const int ResultFieldHeader = 1;
        private const int ColumnFieldHeader = 2;
        private const int ColumnNameFieldHeader = 1;
        private const int RecordFieldHeader = 3;

        private readonly Stack<SubItemToken> tokens = new Stack<SubItemToken>();

        public class TheWriteColumnsMethod : ColumnsWriterTests
        {
            [Fact]
            public void ShouldSerializeColumnName()
            {
                // Arrange
                var columnName = "foo";
                var dataTable = new DataTable();

                dataTable.Columns.Add(columnName, typeof(int));

                dataTable.Rows.Add(1);

                var dataReader = dataTable.CreateDataReader();

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilColumnName();

                Assert.Equal(columnName, reader.ReadString());
            }

            [Fact]
            public void ShouldSerializeExpressionColumn()
            {
                // Arrange
                var columnName = "foo";
                var dataTable = new DataTable();

                dataTable.Columns.Add(columnName, typeof(int));

                dataTable.Rows.Add(1);

                var dataReader = dataTable.CreateDataReader();

                dataReader.GetSchemaTable().Rows[0]["Expression"] = true;

                var options = new ProtoDataWriterOptions() { IncludeComputedColumns = true };

                // Act
                var reader = new ProtoReader(this.Serialize(dataReader, options), null, null);

                // Assert
                var readerContext = new ProtoReaderContext(reader);

                readerContext.ReadUntilColumnName();

                Assert.Equal(columnName, reader.ReadString());
            }

            [Fact]
            public void ShouldNotSerializeExpressionColumn()
            {
                var isRunningOnMono = Type.GetType("Mono.Runtime") != null;

                if (!isRunningOnMono)
                {
                    // Arrange
                    var columnName = "foo";
                    var dataTable = new DataTable();

                    dataTable.Columns.Add(columnName, typeof(int));

                    dataTable.Rows.Add(1);

                    var dataReader = dataTable.CreateDataReader();

                    dataReader.GetSchemaTable().Rows[0]["Expression"] = true;

                    var options = new ProtoDataWriterOptions() { IncludeComputedColumns = false };

                    // Act
                    var reader = new ProtoReader(this.Serialize(dataReader, options), null, null);

                    // Assert
                    var readerContext = new ProtoReaderContext(reader);

                    readerContext.ReadExpectedFieldHeader(ResultFieldHeader);
                    readerContext.StartSubItem();

                    Assert.Equal(RecordFieldHeader, reader.ReadFieldHeader());
                }
            }

            [Fact]
            public void ShouldNotSerializeExpressionColumnOnMono()
            {
                var isRunningOnMono = Type.GetType("Mono.Runtime") != null;

                if (isRunningOnMono)
                {
                    // Arrange
                    var columnName = "foo";
                    var dataTable = new DataTable();

                    dataTable.Columns.Add(columnName, typeof(int));

                    dataTable.Rows.Add(1);

                    var dataReader = dataTable.CreateDataReader();

                    dataReader.GetSchemaTable().Rows[0]["Expression"] = string.Empty;

                    var options = new ProtoDataWriterOptions() { IncludeComputedColumns = false };

                    // Act
                    var reader = new ProtoReader(this.Serialize(dataReader, options), null, null);

                    // Assert
                    var readerContext = new ProtoReaderContext(reader);

                    readerContext.ReadExpectedFieldHeader(ResultFieldHeader);
                    readerContext.StartSubItem();

                    Assert.Equal(RecordFieldHeader, reader.ReadFieldHeader());
                }
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
