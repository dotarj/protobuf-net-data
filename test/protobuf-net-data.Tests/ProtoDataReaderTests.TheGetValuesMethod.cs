// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Data;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataReaderTests
    {
        public class TheGetValuesMethod : ProtoDataReaderTests
        {
            [Fact]
            public void ShouldThrowExceptionWhenValuesIsNull()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: "foo");

                dataReader.Close();

                // Assert
                Assert.Throws<ArgumentNullException>(() => dataReader.GetValues(null));
            }

            [Fact]
            public void ShouldThrowExceptionWhenNoData()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: "foo");

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetValues(new object[1]));
            }

            [Fact]
            public void ShouldThrowExceptionWhenDataReaderIsClosed()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: "foo");

                dataReader.Close();

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetValues(new object[1]));
            }

            [Fact]
            public void ShouldReturnCorrespondingValues()
            {
                // Arrange
                var value = "foo";
                var dataReader = this.CreateDataReader(value: value);

                dataReader.Read();

                var result = new object[1];

                // Act
                dataReader.GetValues(result);

                // Assert
                Assert.Equal(new object[] { value }, result);
            }

            [Fact]
            public void ShouldReturnCorrespondingValuesWithSmallerArray()
            {
                // Arrange
                var dataTable = new DataTable();

                dataTable.Columns.Add("foo", typeof(int));
                dataTable.Columns.Add("bar", typeof(int));

                dataTable.Rows.Add(1, 2);

                var dataReader = this.ToProtoDataReader(dataTable.CreateDataReader());

                dataReader.Read();

                var result = new object[1];

                // Act
                dataReader.GetValues(result);

                // Assert
                Assert.Equal(new[] { dataTable.Rows[0][0] }, result);
            }
        }
    }
}
