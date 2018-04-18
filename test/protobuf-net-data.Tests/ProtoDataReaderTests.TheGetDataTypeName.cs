// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataReaderTests
    {
        public class TheGetDataTypeNameMethod : ProtoDataReaderTests
        {
            [Fact]
            public void ShouldThrowExceptionWhenDataReaderIsClosed()
            {
                // Arrange
                var dataReader = this.CreateDataReader(value: "foo");

                dataReader.Close();

                // Assert
                Assert.Throws<InvalidOperationException>(() => dataReader.GetDataTypeName(0));
            }

            [Fact]
            public void ShouldReturnCorrespondingDataTypeName()
            {
                // Arrange
                var value = "foo";
                var dataReader = this.CreateDataReader(value: value);

                // Act
                var result = dataReader.GetDataTypeName(0);

                // Assert
                Assert.Equal(value.GetType().Name, result);
            }
        }
    }
}
