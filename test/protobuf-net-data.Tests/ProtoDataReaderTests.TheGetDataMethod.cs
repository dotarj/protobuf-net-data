// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Data;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataReaderTests
    {
        public class TheGetDataMethod : ProtoDataReaderTests
        {
            [Fact]
            public void ShouldThrowException()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: true);

                // Assert
                Assert.Throws<NotSupportedException>(() => ((IDataReader)dataReader).GetData(0));
            }
        }
    }
}
