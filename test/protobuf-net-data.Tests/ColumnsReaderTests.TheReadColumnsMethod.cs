// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class ColumnsReaderTests
    {
        public class TheReadColumnsMethod : ColumnsReaderTests
        {
            [Fact]
            public void ShouldThrowExceptionOnColumnsInvalidFieldHeader()
            {
                // Arrange
                var stream = new MemoryStream();

                using (var writer = new ProtoWriter(stream, null, null))
                {
                    ProtoWriter.WriteFieldHeader(1, WireType.StartGroup, writer);

                    ProtoWriter.StartSubItem(0, writer);

                    ProtoWriter.WriteFieldHeader(42, WireType.StartGroup, writer);
                }

                stream.Position = 0;

                // Assert
                Assert.Throws<InvalidDataException>(() => new ProtoDataReader(stream));
            }

            [Fact]
            public void ShouldThrowExceptionOnColumnNameInvalidFieldHeader()
            {
                // Arrange
                var stream = new MemoryStream();

                using (var writer = new ProtoWriter(stream, null, null))
                {
                    ProtoWriter.WriteFieldHeader(1, WireType.StartGroup, writer);

                    var resultToken = ProtoWriter.StartSubItem(0, writer);

                    ProtoWriter.WriteFieldHeader(2, WireType.StartGroup, writer);

                    var columnToken = ProtoWriter.StartSubItem(1, writer);

                    ProtoWriter.WriteFieldHeader(42, WireType.String, writer);
                    ProtoWriter.WriteString("foo", writer);

                    ProtoWriter.EndSubItem(columnToken, writer);

                    ProtoWriter.EndSubItem(resultToken, writer);
                }

                stream.Position = 0;

                // Assert
                Assert.Throws<InvalidDataException>(() => new ProtoDataReader(stream));
            }

            [Fact]
            public void ShouldThrowExceptionOnColumnTypeInvalidFieldHeader()
            {
                // Arrange
                var stream = new MemoryStream();

                using (var writer = new ProtoWriter(stream, null, null))
                {
                    ProtoWriter.WriteFieldHeader(1, WireType.StartGroup, writer);

                    var resultToken = ProtoWriter.StartSubItem(0, writer);

                    ProtoWriter.WriteFieldHeader(2, WireType.StartGroup, writer);

                    var columnToken = ProtoWriter.StartSubItem(1, writer);

                    ProtoWriter.WriteFieldHeader(1, WireType.String, writer);
                    ProtoWriter.WriteString("foo", writer);
                    ProtoWriter.WriteFieldHeader(42, WireType.Variant, writer);
                    ProtoWriter.WriteInt32((int)1, writer);

                    ProtoWriter.EndSubItem(columnToken, writer);

                    ProtoWriter.EndSubItem(resultToken, writer);
                }

                stream.Position = 0;

                // Assert
                Assert.Throws<InvalidDataException>(() => new ProtoDataReader(stream));
            }

            [Fact]
            public void ShouldAcceptTrailingFieldHeaders()
            {
                // Arrange
                var stream = new MemoryStream();

                using (var writer = new ProtoWriter(stream, null, null))
                {
                    ProtoWriter.WriteFieldHeader(1, WireType.StartGroup, writer);

                    var resultToken = ProtoWriter.StartSubItem(0, writer);

                    ProtoWriter.WriteFieldHeader(2, WireType.StartGroup, writer);

                    var columnToken = ProtoWriter.StartSubItem(1, writer);

                    ProtoWriter.WriteFieldHeader(1, WireType.String, writer);
                    ProtoWriter.WriteString("foo", writer);
                    ProtoWriter.WriteFieldHeader(2, WireType.Variant, writer);
                    ProtoWriter.WriteInt32((int)1, writer);
                    ProtoWriter.WriteFieldHeader(42, WireType.String, writer);
                    ProtoWriter.WriteString("bar", writer);

                    ProtoWriter.EndSubItem(columnToken, writer);

                    ProtoWriter.EndSubItem(resultToken, writer);
                }

                stream.Position = 0;

                // Assert
                new ProtoDataReader(stream);
            }
        }
    }
}
