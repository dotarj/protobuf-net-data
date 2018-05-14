// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ResultReaderTests
    {
        public class TheReadResultMethod : ResultReaderTests
        {
            [Fact]
            public void ShouldThrowExceptionOnInvalidFieldHeader()
            {
                // Arrange
                var stream = new MemoryStream();

                using (var writer = new ProtoWriter(stream, null, null))
                {
                    ProtoWriter.WriteFieldHeader(42, WireType.StartGroup, writer);
                }

                stream.Position = 0;

                // Assert
                Assert.Throws<InvalidDataException>(() => new ProtoDataReader(stream));
            }
        }
    }
}
