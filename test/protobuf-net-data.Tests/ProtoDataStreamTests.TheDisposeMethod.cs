// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataStreamTests
    {
        public class TheDisposeMethod : ProtoDataStreamTests
        {
            [Fact]
            public void ShouldDisposeReader()
            {
                // Arrange
                var reader = this.CreateDataReader("foo");
                var stream = new ProtoDataStream(reader);

                // Act
                reader.Dispose();

                // Assert
                Assert.Throws<InvalidOperationException>(() => reader.Read());
            }
        }
    }
}
