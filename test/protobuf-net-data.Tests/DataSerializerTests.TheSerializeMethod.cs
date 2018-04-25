// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System.Data;
using System.IO;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class DataSerializerTests
    {
        public class TheSerializeMethod : DataSerializerTests
        {
            [Fact]
            public void ShouldSerializeDataReader()
            {
                // Arrange
                var stream = new MemoryStream();

                // Act
                DataSerializer.Serialize(stream, this.CreateTable(0).CreateDataReader());

                // Assert
                stream.Position = 0;

                var dataReader = new ProtoDataReader(stream);

                Assert.Equal(1, dataReader.FieldCount);
            }

            [Fact]
            public void ShouldSerializeDataReaderUsingOptions()
            {
                // Arrange
                var stream = new MemoryStream();

                var options = new ProtoDataWriterOptions() { SerializeEmptyArraysAsNull = true };

                // Act
                DataSerializer.Serialize(stream, this.CreateTable(new char[0]).CreateDataReader(), options);

                // Assert
                stream.Position = 0;

                var dataReader = new ProtoDataReader(stream);

                dataReader.Read();

                Assert.True(dataReader.IsDBNull(0));
            }

            [Fact]
            public void ShouldSerializeDataTable()
            {
                // Arrange
                var stream = new MemoryStream();

                // Act
                DataSerializer.Serialize(stream, this.CreateTable(0));

                // Assert
                stream.Position = 0;

                var dataReader = new ProtoDataReader(stream);

                Assert.Equal(1, dataReader.FieldCount);
            }

            [Fact]
            public void ShouldSerializeDataTableUsingOptions()
            {
                // Arrange
                var stream = new MemoryStream();

                var options = new ProtoDataWriterOptions() { SerializeEmptyArraysAsNull = true };

                // Act
                DataSerializer.Serialize(stream, this.CreateTable(new char[0]), options);

                // Assert
                stream.Position = 0;

                var dataReader = new ProtoDataReader(stream);

                dataReader.Read();

                Assert.True(dataReader.IsDBNull(0));
            }

            [Fact]
            public void ShouldSerializeDataSet()
            {
                // Arrange
                var dataSet = new DataSet();

                dataSet.Tables.Add(this.CreateTable(0));

                var stream = new MemoryStream();

                // Act
                DataSerializer.Serialize(stream, dataSet);

                // Assert
                stream.Position = 0;

                var dataReader = new ProtoDataReader(stream);

                Assert.Equal(1, dataReader.FieldCount);
            }

            [Fact]
            public void ShouldSerializeDataSetUsingOptions()
            {
                // Arrange
                var dataSet = new DataSet();

                dataSet.Tables.Add(this.CreateTable(new char[0]));

                var stream = new MemoryStream();

                var options = new ProtoDataWriterOptions() { SerializeEmptyArraysAsNull = true };

                // Act
                DataSerializer.Serialize(stream, dataSet, options);

                // Assert
                stream.Position = 0;

                var dataReader = new ProtoDataReader(stream);

                dataReader.Read();

                Assert.True(dataReader.IsDBNull(0));
            }

            private DataTable CreateTable<TValue>(TValue value)
            {
                var dataTable = new DataTable();

                dataTable.Columns.Add("foo", typeof(TValue));

                dataTable.Rows.Add(value);

                return dataTable;
            }
        }
    }
}
