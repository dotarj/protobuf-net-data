// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Data;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataReaderTests
    {
        public class TheGetOrdinalMethod : ProtoDataReaderTests
        {
            [Fact]
            public void ShouldThrowExceptionWhenDataReaderIsClosed()
            {
                // Arrange
                var dataTable = new DataTable();

                dataTable.Columns.Add("foo", typeof(int));

                var dataReader = this.ToProtoDataReader(dataTable.CreateDataReader());

                dataReader.Close();

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetOrdinal(dataTable.Columns[0].ColumnName));
            }

            [Fact]
            public void ShouldThrowExceptionWhenIndexIsOutOfRange()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: "foo");

                dataReader.Read();

                // Assert
                Assert.Throws<IndexOutOfRangeException>(() => dataReader.GetOrdinal("bar"));
            }

            [Fact]
            public void ShouldReturnCorrespondingOrdinal()
            {
                // Arrange
                var dataTable = new DataTable();

                dataTable.Columns.Add("foo", typeof(int));
                dataTable.Columns.Add("bar", typeof(int));

                var dataReader = this.ToProtoDataReader(dataTable.CreateDataReader());

                // Assert
                Assert.Equal(1, dataReader.GetOrdinal(dataTable.Columns[1].ColumnName));
            }
        }
    }
}
