// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Data;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataReaderTests
    {
        public class TheNextResultMethod : ProtoDataReaderTests
        {
            [Fact]
            public void ShouldThrowExceptionWhenDataReaderIsClosed()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: "foo");

                dataReader.Close();

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.NextResult());
            }

            [Fact]
            public void ShouldNotCloseWhenNoMoreResults()
            {
                // Act
                var dataReader = DataReaderHelper.CreateDataReader(value: (string)null);

                dataReader.NextResult();

                // Assert
                Assert.False(dataReader.IsClosed);
            }

            [Fact]
            public void ShouldReturnFalseWhenNoMoreResults()
            {
                // Act
                var dataReader = DataReaderHelper.CreateDataReader(value: (string)null);

                // Act
                var result = dataReader.NextResult();

                // Assert
                Assert.False(result);
            }

            [Fact]
            public void ShouldReturnFalseWhenMoreResults()
            {
                // Act
                var dataSet = new DataSet();

                dataSet.Tables.Add(new DataTable());
                dataSet.Tables.Add(new DataTable());

                var dataReader = DataReaderHelper.ToProtoDataReader(dataSet.CreateDataReader());

                // Act
                var result = dataReader.NextResult();

                // Assert
                Assert.True(result);
            }

            [Fact]
            public void ShouldReadRemainingRecords()
            {
                // Act
                var dataSet = new DataSet();

                dataSet.Tables.Add(new DataTable());
                dataSet.Tables.Add(new DataTable());

                dataSet.Tables[0].Columns.Add("foo", typeof(string));
                dataSet.Tables[1].Columns.Add("bar", typeof(string));

                dataSet.Tables[0].Rows.Add("baz");
                dataSet.Tables[1].Rows.Add("qux");

                var dataReader = DataReaderHelper.ToProtoDataReader(dataSet.CreateDataReader());

                // Act
                dataReader.NextResult();

                // Assert
                dataReader.Read();

                Assert.Equal(dataSet.Tables[1].Rows[0][0], dataReader.GetValue(0));
            }

            [Fact]
            public void ShouldChangeColumns()
            {
                // Act
                var dataSet = new DataSet();

                dataSet.Tables.Add(new DataTable());
                dataSet.Tables.Add(new DataTable());

                dataSet.Tables[0].Columns.Add("foo", typeof(string));
                dataSet.Tables[1].Columns.Add("bar", typeof(string));

                var dataReader = DataReaderHelper.ToProtoDataReader(dataSet.CreateDataReader());

                // Act
                dataReader.NextResult();

                // Assert
                dataReader.Read();

                Assert.Equal(dataSet.Tables[1].Columns[0].ColumnName, dataReader.GetName(0));
            }
        }
    }
}
