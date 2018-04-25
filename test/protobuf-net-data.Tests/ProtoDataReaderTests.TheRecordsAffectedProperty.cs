// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Data;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataReaderTests
    {
        public class TheRecordsAffectedProperty : ProtoDataReaderTests
        {
            [Fact]
            public void ShouldReturnZero()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: true);

                // Act
                var result = dataReader.RecordsAffected;

                // Assert
                Assert.Equal(0, result);
            }
        }
    }
}
