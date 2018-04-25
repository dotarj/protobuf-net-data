// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataReaderTests
    {
        public class TheGetValueMethod : ProtoDataReaderTests
        {
            [Fact]
            public void ShouldThrowExceptionWhenDataReaderIsClosed()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: "foo");

                dataReader.Close();

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetValue(0));
            }

            [Fact]
            public void ShouldThrowExceptionWhenNoData()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: "foo");

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetValue(0));
            }

            [Fact]
            public void ShouldThrowExceptionWhenIndexIsOutOfRange()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: "foo");

                dataReader.Read();

                // Assert
                Assert.Throws<IndexOutOfRangeException>(() => dataReader.GetValue(dataReader.FieldCount));
            }

            [Fact]
            public void ShouldReturnDbNullWhenNull()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: (string)null);

                dataReader.Read();

                // Act
                var result = dataReader.GetValue(0);

                // Assert
                Assert.Equal(DBNull.Value, result);
            }

            [Fact]
            public void ShouldReturnBoolean()
            {
                // Arrange
                var value = true;
                var dataReader = this.CreateDataReader(value: value);

                dataReader.Read();

                // Act
                var result = dataReader.GetValue(0);

                // Assert
                Assert.Equal(value, result);
            }

            [Fact]
            public void ShouldReturnByte()
            {
                // Arrange
                var value = (byte)0b0010_1010;
                var dataReader = this.CreateDataReader(value: value);

                dataReader.Read();

                // Act
                var result = dataReader.GetValue(0);

                // Assert
                Assert.Equal(value, result);
            }

            [Fact]
            public void ShouldReturnByteArray()
            {
                // Arrange
                var value = new[] { (byte)0b0010_1010 };
                var dataReader = this.CreateDataReader(value: value);

                dataReader.Read();

                // Act
                var result = dataReader.GetValue(0);

                // Assert
                Assert.Equal(value, result);
            }

            [Fact]
            public void ShouldReturnChar()
            {
                // Arrange
                var value = 'z';
                var dataReader = this.CreateDataReader(value: value);

                dataReader.Read();

                // Act
                var result = dataReader.GetValue(0);

                // Assert
                Assert.Equal(value, result);
            }

            [Fact]
            public void ShouldReturnCharArray()
            {
                // Arrange
                var value = new[] { 'a' };
                var dataReader = this.CreateDataReader(value: value);

                dataReader.Read();

                // Act
                var result = dataReader.GetValue(0);

                // Assert
                Assert.Equal(value, result);
            }

            [Fact]
            public void ShouldReturnDateTime()
            {
                // Arrange
                var value = DateTime.Now;
                var dataReader = this.CreateDataReader(value: value);

                dataReader.Read();

                // Act
                var result = dataReader.GetValue(0);

                // Assert
                Assert.Equal(value, result);
            }

            [Fact]
            public void ShouldReturnDecimal()
            {
                // Arrange
                var value = decimal.MinValue;
                var dataReader = this.CreateDataReader(value: value);

                dataReader.Read();

                // Act
                var result = dataReader.GetValue(0);

                // Assert
                Assert.Equal(value, result);
            }

            [Fact]
            public void ShouldReturnDouble()
            {
                // Arrange
                var value = double.MinValue;
                var dataReader = this.CreateDataReader(value: value);

                dataReader.Read();

                // Act
                var result = dataReader.GetValue(0);

                // Assert
                Assert.Equal(value, result);
            }

            [Fact]
            public void ShouldReturnFloat()
            {
                // Arrange
                var value = float.MinValue;
                var dataReader = this.CreateDataReader(value: value);

                dataReader.Read();

                // Act
                var result = dataReader.GetValue(0);

                // Assert
                Assert.Equal(value, result);
            }

            [Fact]
            public void ShouldReturnGuid()
            {
                // Arrange
                var value = Guid.NewGuid();
                var dataReader = this.CreateDataReader(value: value);

                dataReader.Read();

                // Act
                var result = dataReader.GetValue(0);

                // Assert
                Assert.Equal(value, result);
            }

            [Fact]
            public void ShouldReturnInt16()
            {
                // Arrange
                var value = short.MinValue;
                var dataReader = this.CreateDataReader(value: value);

                dataReader.Read();

                // Act
                var result = dataReader.GetValue(0);

                // Assert
                Assert.Equal(value, result);
            }

            [Fact]
            public void ShouldReturnInt32()
            {
                // Arrange
                var value = int.MinValue;
                var dataReader = this.CreateDataReader(value: value);

                dataReader.Read();

                // Act
                var result = dataReader.GetValue(0);

                // Assert
                Assert.Equal(value, result);
            }

            [Fact]
            public void ShouldReturnInt64()
            {
                // Arrange
                var value = long.MinValue;
                var dataReader = this.CreateDataReader(value: value);

                dataReader.Read();

                // Act
                var result = dataReader.GetValue(0);

                // Assert
                Assert.Equal(value, result);
            }

            [Fact]
            public void ShouldReturnString()
            {
                // Arrange
                var value = "foo";
                var dataReader = this.CreateDataReader(value: value);

                dataReader.Read();

                // Act
                var result = dataReader.GetValue(0);

                // Assert
                Assert.Equal(value, result);
            }

            [Fact]
            public void ShouldReturnTimeSpan()
            {
                // Arrange
                var value = TimeSpan.FromTicks(1);
                var dataReader = this.CreateDataReader(value: value);

                dataReader.Read();

                // Act
                var result = dataReader.GetValue(0);

                // Assert
                Assert.Equal(value, result);
            }
        }
    }
}
