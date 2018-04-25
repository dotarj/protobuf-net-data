// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataReaderTests
    {
        public class TheGetStringMethod : ProtoDataReaderTests
        {
            [Fact]
            public void ShouldThrowExceptionWhenDataReaderIsClosed()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: "foo");

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetString(0));
            }

            [Fact]
            public void ShouldThrowExceptionWhenNoData()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: "foo");

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetString(0));
            }

            [Fact]
            public void ShouldThrowExceptionWhenIndexIsOutOfRange()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: "foo");

                dataReader.Read();

                // Assert
                Assert.Throws<IndexOutOfRangeException>(() => dataReader.GetString(dataReader.FieldCount));
            }

            [Fact]
            public void ShouldThrowExceptionWhenIsNull()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: (string)null);

                dataReader.Read();

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetString(0));
            }

            [Fact]
            public void ShouldThrowExceptionIfInvalidType()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: 42);

                dataReader.Read();

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetString(0));
            }

            [Fact]
            public void ShouldReturnCorrespondingValue()
            {
                // Arrange
                var value = "foo";
                var dataReader = this.CreateDataReader(value: value);

                dataReader.Read();

                // Act
                var result = dataReader.GetString(0);

                // Assert
                Assert.Equal(value, result);
            }
        }
    }
}
