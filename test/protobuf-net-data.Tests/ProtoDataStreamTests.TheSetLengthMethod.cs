// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataStreamTests
    {
        public class TheSSetLengthMethod : ProtoDataStreamTests
        {
            [Fact]
            public void ShouldThrowException()
            {
                // Arrange
                var stream = new ProtoDataStream(this.CreateDataReader("foo"));

                // Assert
                Assert.Throws<NotSupportedException>(() => stream.SetLength(0));
            }
        }
    }
}
