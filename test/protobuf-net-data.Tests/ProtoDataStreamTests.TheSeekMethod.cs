// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.IO;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataStreamTests
    {
        public class TheSeekMethod : ProtoDataStreamTests
        {
            [Fact]
            public void ShouldThrowException()
            {
                // Arrange
                var stream = new ProtoDataStream(this.CreateDataReader("foo"));

                // Assert
                Assert.Throws<NotSupportedException>(() => stream.Seek(0, SeekOrigin.Begin));
            }
        }
    }
}
