// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataReaderTests
    {
        public class TheGetInt32Method : ProtoDataReaderTests
        {
            [Fact]
            public void ShouldThrowExceptionWhenDataReaderIsClosed()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: int.MinValue);

                dataReader.Close();

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetInt32(0));
            }

            [Fact]
            public void ShouldThrowExceptionWhenNoData()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: int.MinValue);

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetInt32(0));
            }

            [Fact]
            public void ShouldThrowExceptionWhenIndexIsOutOfRange()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: int.MinValue);

                dataReader.Read();

                // Assert
                Assert.Throws<IndexOutOfRangeException>(() => dataReader.GetInt32(dataReader.FieldCount));
            }

            [Fact]
            public void ShouldThrowExceptionWhenIsNull()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: (string)null);

                dataReader.Read();

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetInt32(0));
            }

            [Fact]
            public void ShouldThrowExceptionIfInvalidType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: "foo");

                dataReader.Read();

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetInt32(0));
            }

            [Fact]
            public void ShouldReturnCorrespondingValue()
            {
                // Arrange
                var value = int.MinValue;
                var dataReader = this.CreateDataReader(value: value);

                dataReader.Read();

                // Act
                var result = dataReader.GetInt32(0);

                // Assert
                Assert.Equal(value, result);
            }
        }
    }
}
