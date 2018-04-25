// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataWriterOptionsTests
    {
        public class TheEqualsMethod : ProtoDataWriterOptionsTests
        {
            [Fact]
            public void ShouldReturnFalseIfOtherIsNull()
            {
                // Arrange
                var options = new ProtoDataWriterOptions();

                // Act
                var result = options.Equals(null);

                // Assert
                Assert.False(result);
            }

            [Fact]
            public void ShouldReturnTrueIfSameInstance()
            {
                // Arrange
                var options = new ProtoDataWriterOptions();

                // Act
                var result = options.Equals(options);

                // Assert
                Assert.True(result);
            }

            [Fact]
            public void ShouldReturnFalseIfSerializeEmptyArraysAsNullDiffers()
            {
                // Arrange
                var options = new ProtoDataWriterOptions() { SerializeEmptyArraysAsNull = true };
                var other = new ProtoDataWriterOptions() { SerializeEmptyArraysAsNull = false };

                // Act
                var result = options.Equals(other);

                // Assert
                Assert.False(result);
            }

            [Fact]
            public void ShouldReturnFalseIfIncludeComputedColumnsDiffers()
            {
                // Arrange
                var options = new ProtoDataWriterOptions() { IncludeComputedColumns = true };
                var other = new ProtoDataWriterOptions() { IncludeComputedColumns = false };

                // Act
                var result = options.Equals(other);

                // Assert
                Assert.False(result);
            }

            [Fact]
            public void ShouldReturnTrueIfValuesAreSame()
            {
                // Arrange
                var options = new ProtoDataWriterOptions() { IncludeComputedColumns = true, SerializeEmptyArraysAsNull = true };
                var other = new ProtoDataWriterOptions() { IncludeComputedColumns = true, SerializeEmptyArraysAsNull = true };

                // Act
                var result = options.Equals(other);

                // Assert
                Assert.True(result);
            }

            [Fact]
            public void ShouldReturnFalseIfTypeDiffers()
            {
                // Arrange
                var options = new ProtoDataWriterOptions() { IncludeComputedColumns = true, SerializeEmptyArraysAsNull = true };
                var other = new { };

                // Act
                var result = options.Equals(other);

                // Assert
                Assert.False(result);
            }
        }
    }
}
