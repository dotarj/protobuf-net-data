// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Data;
using Moq;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataStreamTests
    {
        public class TheConstructor : ProtoDataStreamTests
        {
            [Fact]
            public void ShouldThrowIfReaderIsNull()
            {
                // Assert
                Assert.Throws<ArgumentNullException>(() => new ProtoDataStream(reader: null, options: new ProtoDataWriterOptions()));
            }

            [Fact]
            public void ShouldThrowIfOptionsIsNull()
            {
                // Assert
                Assert.Throws<ArgumentNullException>(() => new ProtoDataStream(reader: Mock.Of<IDataReader>(), options: null));
            }
        }
    }
}
