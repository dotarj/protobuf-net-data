// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataWriterOptionsTests
    {
        public class TheGetHashCodeMethod : ProtoDataWriterOptionsTests
        {
            [Fact]
            public void ShouldReturnFalseIfOtherIsNull()
            {
                // Arrange
                var options = new ProtoDataWriterOptions() { IncludeComputedColumns = true, SerializeEmptyArraysAsNull = true };

                // Act
                var result = options.GetHashCode();

                // Assert
                Assert.Equal(396, result);
            }
        }
    }
}
