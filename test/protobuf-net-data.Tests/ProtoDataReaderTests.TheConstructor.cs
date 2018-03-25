// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Data;
using System.IO;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataReaderTests
    {
        public class TheConstructor : ProtoDataReaderTests
        {
            [Fact]
            public void ShouldThrowIfStreamIsNull()
            {
                // Assert
                Assert.Throws<ArgumentNullException>(() => new ProtoDataReader(null));
            }
        }
    }
}
