// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataReaderTests
    {
        public class TheGetFieldTypeMethod : ProtoDataReaderTests
        {
            [Fact]
            public void ShouldThrowExceptionWhenDataReaderIsClosed()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: "foo");

                dataReader.Close();

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetFieldType(0));
            }

            [Fact]
            public void ShouldThrowExceptionWhenIndexIsOutOfRange()
            {
                // Arrange
                var dataReader = DataReaderHelper.CreateDataReader(value: "foo");

                dataReader.Read();

                // Assert
                Assert.Throws<IndexOutOfRangeException>(() => dataReader.GetFieldType(dataReader.FieldCount));
            }

            [Fact]
            public void ShouldReturnCorrespondingFieldType()
            {
                // Arrange
                var value = "foo";
                var dataReader = DataReaderHelper.CreateDataReader(value: value);

                dataReader.Read();

                // Act
                var result = dataReader.GetFieldType(0);

                // Assert
                Assert.Equal(value.GetType(), result);
            }
        }
    }
}
