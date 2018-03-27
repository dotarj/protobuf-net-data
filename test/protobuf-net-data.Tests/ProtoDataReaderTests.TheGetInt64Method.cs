// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataReaderTests
    {
        public class TheGetInt64Method : ProtoDataReaderTests
        {
            [Fact]
            public void ShouldThrowExceptionWhenDataReaderIsClosed()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: long.MinValue);

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetInt64(0));
            }

            [Fact]
            public void ShouldThrowExceptionWhenNoData()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: long.MinValue);

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetInt64(0));
            }

            [Fact]
            public void ShouldThrowExceptionWhenIndexIsOutOfRange()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: long.MinValue);

                dataReader.Read();

                // Assert
                Assert.Throws<IndexOutOfRangeException>(() => dataReader.GetInt64(dataReader.FieldCount));
            }

            [Fact]
            public void ShouldThrowExceptionWhenIsNull()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: (string)null);

                dataReader.Read();

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetInt64(0));
            }

            [Fact]
            public void ShouldReturnCorrespondingValue()
            {
                // Arrange
                var value = long.MinValue;
                var dataReader = DataReaderHelper.CreateDataReader(value: value);

                dataReader.Read();

                // Assert
                Assert.Equal(value, dataReader.GetInt64(0));
            }
        }
    }
}
