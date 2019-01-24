// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Data;
using System.IO;
using Moq;
using ProtoBuf.Data;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class DataSerializerEngineTests
    {
        public class TheSerializeMethod : DataSerializerEngineTests
        {
            [Fact]
            public void ShouldThrowExceptionIfStreamIsNull1()
            {
                // Arrange
                var engine = new DataSerializerEngine();

                // Assert
                Assert.Throws<ArgumentNullException>(() => engine.Serialize(null, Mock.Of<IDataReader>(), new ProtoDataWriterOptions()));
            }

            [Fact]
            public void ShouldThrowExceptionIfStreamIsNull2()
            {
                // Arrange
                var engine = new DataSerializerEngine();

                // Assert
                Assert.Throws<ArgumentNullException>(() => engine.Serialize(null, new DataTable(), new ProtoDataWriterOptions()));
            }

            [Fact]
            public void ShouldThrowExceptionIfStreamIsNull3()
            {
                // Arrange
                var engine = new DataSerializerEngine();

                // Assert
                Assert.Throws<ArgumentNullException>(() => engine.Serialize(null, new DataSet(), new ProtoDataWriterOptions()));
            }

            [Fact]
            public void ShouldThrowExceptionIfDataReaderIsNull()
            {
                // Arrange
                var engine = new DataSerializerEngine();

                // Assert
                Assert.Throws<ArgumentNullException>(() => engine.Serialize(Mock.Of<Stream>(), (IDataReader)null, new ProtoDataWriterOptions()));
            }

            [Fact]
            public void ShouldThrowExceptionIfDataTableIsNull()
            {
                // Arrange
                var engine = new DataSerializerEngine();

                // Assert
                Assert.Throws<ArgumentNullException>(() => engine.Serialize(Mock.Of<Stream>(), (DataTable)null, new ProtoDataWriterOptions()));
            }

            [Fact]
            public void ShouldThrowExceptionIfDataSetIsNull()
            {
                // Arrange
                var engine = new DataSerializerEngine();

                // Assert
                Assert.Throws<ArgumentNullException>(() => engine.Serialize(Mock.Of<Stream>(), (DataSet)null, new ProtoDataWriterOptions()));
            }
        }
    }
}
