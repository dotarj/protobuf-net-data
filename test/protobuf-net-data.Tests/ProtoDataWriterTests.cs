// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System.IO;

namespace ProtoBuf.Data.Tests
{
    public partial class ProtoDataWriterTests
    {
        private readonly MemoryStream stream = new MemoryStream();
        private readonly ProtoDataWriter writer = new ProtoDataWriter();
        private readonly ProtoReader reader;

        public ProtoDataWriterTests()
        {
            this.reader = new ProtoReader(this.stream, null, null);
        }
    }
}
