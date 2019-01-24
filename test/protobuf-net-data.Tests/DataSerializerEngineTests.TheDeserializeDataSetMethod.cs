// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.IO;
using Moq;
using ProtoBuf.Data;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class DataSerializerEngineTests
    {
        public class TheDeserializeDataSetMethod : DataSerializerEngineTests
        {
            [Fact]
            public void ShouldThrowExceptionIfStreamIsNull1()
            {
                // Arrange
                var engine = new DataSerializerEngine();

                // Assert
                Assert.Throws<ArgumentNullException>(() => engine.DeserializeDataSet(null, "foo"));
            }

            [Fact]
            public void ShouldThrowExceptionIfStreamIsNull2()
            {
                // Arrange
                var engine = new DataSerializerEngine();

                // Assert
                Assert.Throws<ArgumentNullException>(() => engine.DeserializeDataSet(null, new List<string> { "foo" }));
            }

            [Fact]
            public void ShouldThrowExceptionIfTablesIsNull1()
            {
                // Arrange
                var engine = new DataSerializerEngine();

                // Assert
                Assert.Throws<ArgumentNullException>(() => engine.DeserializeDataSet(Mock.Of<Stream>(), (string[])null));
            }

            [Fact]
            public void ShouldThrowExceptionIfTablesIsNull2()
            {
                // Arrange
                var engine = new DataSerializerEngine();

                // Assert
                Assert.Throws<ArgumentNullException>(() => engine.DeserializeDataSet(Mock.Of<Stream>(), (IEnumerable<string>)null));
            }
        }
    }
}
