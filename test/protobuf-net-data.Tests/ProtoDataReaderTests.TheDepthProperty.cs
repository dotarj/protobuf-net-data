// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Data;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataReaderTests
    {
        public class TheDepthProperty : ProtoDataReaderTests
        {
            [Fact]
            public void ShouldThrowExceptionWhenDataReaderIsClosed()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: true);

                dataReader.Close();

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.Depth);
            }

            [Fact]
            public void ShouldReturnZero()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: true);

                dataReader.Read();

                // Act
                var result = dataReader.Depth;

                // Assert
                Assert.Equal(0, result);
            }
        }
    }
}
