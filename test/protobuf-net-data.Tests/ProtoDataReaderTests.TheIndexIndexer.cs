// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataReaderTests
    {
        public class TheIndexIndexer : ProtoDataReaderTests
        {
            [Fact]
            public void ShouldThrowExceptionWhenDataReaderIsClosed()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: "foo");

                dataReader.Close();

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader[0]);
            }

            [Fact]
            public void ShouldThrowExceptionWhenNoData()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: "foo");

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader[0]);
            }

            [Fact]
            public void ShouldThrowExceptionWhenIndexIsOutOfRange()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: "foo");

                dataReader.Read();

                // Assert
                Assert.Throws<IndexOutOfRangeException>(() => dataReader[dataReader.FieldCount]);
            }

            [Fact]
            public void ShouldReturnCorrespondingValue()
            {
                // Arrange
                var value = "foo";
                var dataReader = DataReaderHelper.CreateDataReader(value: value);

                dataReader.Read();

                // Act
                var result = dataReader[0];

                // Assert
                Assert.Equal(value, Convert.ToString(result));
            }
        }
    }
}
