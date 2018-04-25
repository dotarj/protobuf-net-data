// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System.Collections.Generic;
using System.Data;
using System.IO;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class DataSerializerTests
    {
        public class TheDeserializeMethod : DataSerializerTests
        {
            [Fact]
            public void ShouldDeserializeDataReader()
            {
                // Arrange
                var stream = new MemoryStream();

                DataSerializer.Serialize(stream, this.CreateTable(0).CreateDataReader());

                stream.Position = 0;

                // Act
                var dataReader = DataSerializer.Deserialize(stream);

                // Assert
                Assert.Equal(1, dataReader.FieldCount);
            }

            [Fact]
            public void ShouldDeserializeDataTable()
            {
                // Arrange
                var stream = new MemoryStream();

                DataSerializer.Serialize(stream, this.CreateTable(0));

                stream.Position = 0;

                // Act
                var dataTable = DataSerializer.DeserializeDataTable(stream);

                // Assert
                Assert.Single(dataTable.Columns);
            }

            [Fact]
            public void ShouldDeserializeDataSet()
            {
                // Arrange
                var dataSet = new DataSet();

                dataSet.Tables.Add(this.CreateTable(0));

                var stream = new MemoryStream();

                DataSerializer.Serialize(stream, dataSet);

                stream.Position = 0;

                // Act
                var result = DataSerializer.DeserializeDataSet(stream, "bar");

                // Assert
                Assert.Single(result.Tables);
            }

            [Fact]
            public void ShouldDeserializeDataSet2()
            {
                // Arrange
                var dataSet = new DataSet();

                dataSet.Tables.Add(this.CreateTable(0));

                var stream = new MemoryStream();

                DataSerializer.Serialize(stream, dataSet);

                stream.Position = 0;

                // Act
                var result = DataSerializer.DeserializeDataSet(stream, new List<string> { "bar" });

                // Assert
                Assert.Single(result.Tables);
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
