// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Data;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataReaderTests
    {
        public class TheGetNameMethod : ProtoDataReaderTests
        {
            [Fact]
            public void ShouldThrowExceptionWhenDataReaderIsClosed()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: "foo");

                dataReader.Close();

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetName(0));
            }

            [Fact]
            public void ShouldThrowExceptionWhenIndexIsOutOfRange()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: "foo");

                dataReader.Read();

                // Assert
                Assert.Throws<IndexOutOfRangeException>(() => dataReader.GetName(dataReader.FieldCount));
            }

            [Fact]
            public void ShouldReturnCorrespondingName()
            {
                // Arrange
                var dataTable = new DataTable();

                dataTable.Columns.Add("foo", typeof(int));

                var dataReader = DataReaderHelper.ToProtoDataReader(dataTable.CreateDataReader());

                // Act
                var result = dataReader.GetName(0);

                // Assert
                Assert.Equal(dataTable.Columns[0].ColumnName, result);
            }
        }
    }
}
