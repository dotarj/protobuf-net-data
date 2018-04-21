// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Data;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataReaderTests
    {
        public class TheGetSchemaTableMethod : ProtoDataReaderTests
        {
            [Fact]
            public void ShouldThrowExceptionWhenDataReaderIsClosed()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: "foo");

                dataReader.Close();

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetSchemaTable());
            }

            [Fact]
            public void ShouldSetTableName()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: "foo");

                // Act
                var schemaTable = dataReader.GetSchemaTable();

                // Assert
                Assert.Equal("SchemaTable", schemaTable.TableName);
            }

            [Fact]
            public void ShouldSetColumnName()
            {
                // Arrange
                var dataTable = new DataTable();

                dataTable.Columns.Add("foo", typeof(int));
                dataTable.Columns.Add("bar", typeof(string));

                var dataReader = this.ToProtoDataReader(dataTable.CreateDataReader());

                // Act
                var schemaTable = dataReader.GetSchemaTable();

                // Assert
                Assert.Equal(dataTable.Columns[1].ColumnName, schemaTable.Rows[1]["ColumnName"]);
            }

            [Fact]
            public void ShouldSetColumnOrdinal()
            {
                // Arrange
                var dataTable = new DataTable();

                dataTable.Columns.Add("foo", typeof(int));
                dataTable.Columns.Add("bar", typeof(string));

                var dataReader = this.ToProtoDataReader(dataTable.CreateDataReader());

                // Act
                var schemaTable = dataReader.GetSchemaTable();

                // Assert
                Assert.Equal(1, schemaTable.Rows[1]["ColumnOrdinal"]);
            }

            [Fact]
            public void ShouldSetDefaultColumnSize()
            {
                // Arrange
                var dataTable = new DataTable();

                dataTable.Columns.Add("foo", typeof(int));
                dataTable.Columns.Add("bar", typeof(string));

                var dataReader = this.ToProtoDataReader(dataTable.CreateDataReader());

                // Act
                var schemaTable = dataReader.GetSchemaTable();

                // Assert
                Assert.Equal(-1, schemaTable.Rows[1]["ColumnSize"]);
            }

            [Fact]
            public void ShouldSetDataType()
            {
                // Arrange
                var dataTable = new DataTable();

                dataTable.Columns.Add("foo", typeof(int));
                dataTable.Columns.Add("bar", typeof(string));

                var dataReader = this.ToProtoDataReader(dataTable.CreateDataReader());

                // Act
                var schemaTable = dataReader.GetSchemaTable();

                // Assert
                Assert.Equal(dataTable.Columns[1].DataType, schemaTable.Rows[1]["DataType"]);
            }
        }
    }
}
