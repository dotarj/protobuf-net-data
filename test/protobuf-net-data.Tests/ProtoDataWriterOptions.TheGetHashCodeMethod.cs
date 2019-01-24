// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataWriterOptionsTests
    {
        public class TheGetHashCodeMethod : ProtoDataWriterOptionsTests
        {
            [Fact]
            public void ShouldReturnDifferentHashCodeIfIncludeComputedColumnsDiffers()
            {
                // Arrange
                var options1 = new ProtoDataWriterOptions() { IncludeComputedColumns = true, SerializeEmptyArraysAsNull = true };
                var options2 = new ProtoDataWriterOptions() { IncludeComputedColumns = false, SerializeEmptyArraysAsNull = true };

                // Act
                var result1 = options1.GetHashCode();
                var result2 = options2.GetHashCode();

                // Assert
                Assert.NotEqual(result1, result2);
            }

            [Fact]
            public void ShouldReturnDifferentHashCodeIfSerializeEmptyArraysAsNullDiffers()
            {
                // Arrange
                var options1 = new ProtoDataWriterOptions() { IncludeComputedColumns = true, SerializeEmptyArraysAsNull = true };
                var options2 = new ProtoDataWriterOptions() { IncludeComputedColumns = true, SerializeEmptyArraysAsNull = false };

                // Act
                var result1 = options1.GetHashCode();
                var result2 = options2.GetHashCode();

                // Assert
                Assert.NotEqual(result1, result2);
            }

            [Fact]
            public void ShouldReturnDifferentHashCodeIfIncludeComputedColumnsAndSerializeEmptyArraysAsNullDiffers()
            {
                // Arrange
                var options1 = new ProtoDataWriterOptions() { IncludeComputedColumns = true, SerializeEmptyArraysAsNull = true };
                var options2 = new ProtoDataWriterOptions() { IncludeComputedColumns = false, SerializeEmptyArraysAsNull = false };

                // Act
                var result1 = options1.GetHashCode();
                var result2 = options2.GetHashCode();

                // Assert
                Assert.NotEqual(result1, result2);
            }

            [Fact]
            public void ShouldReturnSameHashCodeIfIncludeComputedColumnsAndSerializeEmptyArraysAsNullAreSame()
            {
                // Arrange
                var options1 = new ProtoDataWriterOptions() { IncludeComputedColumns = true, SerializeEmptyArraysAsNull = true };
                var options2 = new ProtoDataWriterOptions() { IncludeComputedColumns = true, SerializeEmptyArraysAsNull = true };

                // Act
                var result1 = options1.GetHashCode();
                var result2 = options2.GetHashCode();

                // Assert
                Assert.Equal(result1, result2);
            }
        }
    }
}
