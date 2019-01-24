// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using ProtoBuf.Data;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class DataSerializerEngineTests
    {
        public class TheDeserializeMethod : DataSerializerEngineTests
        {
            [Fact]
            public void ShouldThrowExceptionIfStreamIsNull()
            {
                // Arrange
                var engine = new DataSerializerEngine();

                // Assert
                Assert.Throws<ArgumentNullException>(() => engine.Deserialize(null));
            }
        }
    }
}
