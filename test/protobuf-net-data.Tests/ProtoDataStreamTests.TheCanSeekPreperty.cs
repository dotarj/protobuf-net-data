// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataStreamTests
    {
        public class TheCanSeekProperty : ProtoDataStreamTests
        {
            [Fact]
            public void ShouldReturnFalse()
            {
                // Arrange
                var stream = new ProtoDataStream(this.CreateDataReader("foo"));

                // Act
                var result = stream.CanSeek;

                // Assert
                Assert.False(result);
            }
        }
    }
}
