// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataStreamTests
    {
        public class TheCanReadProperty : ProtoDataStreamTests
        {
            [Fact]
            public void ShouldReturnTrueIfNotDisposed()
            {
                // Arrange
                var stream = new ProtoDataStream(DataReaderHelper.CreateDataReader("foo"));

                // Act
                var result = stream.CanRead;

                // Assert
                Assert.True(result);
            }

            [Fact]
            public void ShouldReturnFalseIfDisposed()
            {
                // Arrange
                var stream = new ProtoDataStream(DataReaderHelper.CreateDataReader("foo"));

                stream.Dispose();

                // Act
                var result = stream.CanRead;

                // Assert
                Assert.False(result);
            }
        }
    }
}
