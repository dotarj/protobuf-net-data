// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Data;
using System.IO;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataReaderTests
    {
        public class TheDisposeMethod : ProtoDataReaderTests
        {
            [Fact]
            public void ShouldDisposeStream()
            {
                // Arrange
                var dataTable = new DataTable();

                dataTable.Columns.Add("foo", typeof(int));

                var memoryStream = new MemoryStream();

                DataSerializer.Serialize(memoryStream, dataTable.CreateDataReader());

                memoryStream.Position = 0;

                var dataReader = (ProtoDataReader)DataSerializer.Deserialize(memoryStream);

                // Act
                dataReader.Dispose();

                // Assert
                Assert.Throws<ObjectDisposedException>(() => memoryStream.Position = 0);
            }
        }
    }
}
