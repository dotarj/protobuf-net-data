// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public partial class RecordReaderTests
    {
        public class TheReadRecordMethod : RecordReaderTests
        {
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
                    ProtoWriter.WriteInt32((int)3, writer);

                    ProtoWriter.EndSubItem(columnToken, writer);

                    ProtoWriter.WriteFieldHeader(3, WireType.StartGroup, writer);

                    var recordToken = ProtoWriter.StartSubItem(1, writer);

                    ProtoWriter.WriteFieldHeader(1, WireType.Variant, writer);
                    ProtoWriter.WriteInt32((int)1, writer);
                    ProtoWriter.WriteFieldHeader(2, WireType.Variant, writer);
                    ProtoWriter.WriteInt32((int)1, writer);

                    ProtoWriter.EndSubItem(recordToken, writer);

                    ProtoWriter.EndSubItem(resultToken, writer);
                }

                stream.Position = 0;

                var dataReader = new ProtoDataReader(stream);

                // Assert
                dataReader.Read();
            }
        }
    }
}
