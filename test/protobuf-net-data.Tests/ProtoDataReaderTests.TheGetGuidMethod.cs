// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataReaderTests
    {
        public class TheGetGuidMethod : ProtoDataReaderTests
        {
            [Fact]
            public void ShouldThrowExceptionWhenDataReaderIsClosed()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: Guid.NewGuid());

                dataReader.Close();

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetGuid(0));
            }

            [Fact]
            public void ShouldThrowExceptionWhenNoData()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: Guid.NewGuid());

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetGuid(0));
            }

            [Fact]
            public void ShouldThrowExceptionWhenIndexIsOutOfRange()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: Guid.NewGuid());

                dataReader.Read();

                // Assert
                Assert.Throws<IndexOutOfRangeException>(() => dataReader.GetGuid(dataReader.FieldCount));
            }

            [Fact]
            public void ShouldThrowExceptionWhenIsNull()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: (string)null);

                dataReader.Read();

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetGuid(0));
            }

            [Fact]
            public void ShouldReturnCorrespondingValue()
            {
                // Arrange
                var value = Guid.NewGuid();
                var dataReader = DataReaderHelper.CreateDataReader(value: value);

                dataReader.Read();

                // Act
                var result = dataReader.GetGuid(0);

                // Assert
                Assert.Equal(value, result);
            }
        }
    }
}
