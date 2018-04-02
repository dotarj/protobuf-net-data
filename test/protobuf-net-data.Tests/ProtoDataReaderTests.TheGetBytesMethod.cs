// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataReaderTests
    {
        public class TheGetBytesMethod : ProtoDataReaderTests
        {
            [Fact]
            public void ShouldThrowExceptionWhenDataReaderIsClosed()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: new[] { (byte)0b0010_1010 });

                dataReader.Close();

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetBytes(0, 0, new byte[0], 0, 1));
            }

            [Fact]
            public void ShouldThrowExceptionWhenNoData()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: new[] { (byte)0b0010_1010 });

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetBytes(0, 0, new byte[1], 0, 1));
            }

            [Fact]
            public void ShouldThrowExceptionWhenIndexIsOutOfRange()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: new[] { (byte)0b0010_1010 });

                dataReader.Read();

                // Assert
                Assert.Throws<IndexOutOfRangeException>(() => dataReader.GetBytes(dataReader.FieldCount, 0, new byte[0], 0, 1));
            }

            [Fact]
            public void ShouldThrowExceptionWhenIsNull()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: (string)null);

                dataReader.Read();

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetBytes(0, 0, new byte[1], 0, 1));
            }

            [Fact]
            public void ShouldThrowExceptionWhenFieldOffsetIsLessThanZero()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: new[] { (byte)0b0010_1010 });

                dataReader.Read();

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetBytes(0, -1, new byte[1], 0, 1));
            }

            [Fact]
            public void ShouldThrowExceptionWhenLengthIsLessThanZero()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: new[] { (byte)0b0010_1010 });

                dataReader.Read();

                // Assert
                Assert.Throws<IndexOutOfRangeException>(() => dataReader.GetBytes(0, 0, new byte[1], 0, -1));
            }

            [Fact]
            public void ShouldThrowExceptionWhenBufferOffsetIsLessThanZero()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: new[] { (byte)0b0010_1010 });

                dataReader.Read();

                // Assert
                Assert.Throws<ArgumentOutOfRangeException>(() => dataReader.GetBytes(0, 0, new byte[1], -1, 1));
            }

            [Fact]
            public void ShouldThrowExceptionWhenBufferOffsetIsGreaterThanBufferSize()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: new[] { (byte)0b0010_1010 });

                dataReader.Read();

                // Assert
                Assert.Throws<ArgumentOutOfRangeException>(() => dataReader.GetBytes(0, 0, new byte[1], 10, 1));
            }

            [Fact]
            public void ShouldThrowExceptionWhenBufferOffsetIsEqualToBufferSize()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: new[] { (byte)0b0010_1010 });

                dataReader.Read();

                // Assert
                Assert.Throws<ArgumentOutOfRangeException>(() => dataReader.GetBytes(0, 0, new byte[1], 1, 1));
            }

            [Fact]
            public void ShouldReturnByteArrayLengthWhenBufferIsNull()
            {
                // Arrange
                var value = new[] { (byte)0b0010_1010 };
                var dataReader = DataReaderHelper.CreateDataReader(value: value);

                dataReader.Read();

                // Act
                var result = dataReader.GetBytes(0, 0, null, 0, 1);

                // Assert
                Assert.Equal(value.Length, result);
            }

            [Fact]
            public void ShouldThrowExceptionWhenByteArrayLengthAndBufferOffsetIsGreaterThanBufferLength()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: new[] { (byte)0b0010_1000, (byte)0b1010 });

                dataReader.Read();

                // Assert
                Assert.Throws<IndexOutOfRangeException>(() => dataReader.GetBytes(0, 0, new byte[2], 1, 0));
            }

            [Fact]
            public void ShouldReturnZeroWhenFieldOffsetIsGreaterThanByteArrayLength()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: new[] { (byte)0b0010_1010 });

                dataReader.Read();

                // Act
                var copyLength = dataReader.GetBytes(0, 1, new byte[1], 0, 1);

                // Assert
                Assert.Equal(0, copyLength);
            }

            [Fact]
            public void ShouldAdjustCopyLengthWhenFieldOffsetAndLengthExceedsByteArrayLength()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: new[] { (byte)0b0010_1010 });

                dataReader.Read();

                // Act
                var copyLength = dataReader.GetBytes(0, 0, new byte[1], 0, 2);

                // Assert
                Assert.Equal(1, copyLength);
            }

            [Fact]
            public void ShouldReturnCorrespondingValue()
            {
                // Arrange
                var value = new[] { (byte)0b0010_1010 };
                var dataReader = DataReaderHelper.CreateDataReader(value: value);

                dataReader.Read();

                var buffer = new byte[1];

                // Act
                var copyLength = dataReader.GetBytes(0, 0, buffer, 0, 1);

                // Assert
                Assert.Equal(value, buffer);
            }
        }
    }
}
