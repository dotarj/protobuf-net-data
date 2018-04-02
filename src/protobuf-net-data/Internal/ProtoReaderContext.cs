// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System.Collections.Generic;
using System.IO;

namespace ProtoBuf.Data.Internal
{
    internal class ProtoReaderContext
    {
        private readonly Stack<SubItemToken> subItemTokens = new Stack<SubItemToken>();

        public ProtoReaderContext(ProtoReader reader)
        {
            this.Reader = reader;
        }

        public ProtoReader Reader { get; }

        public List<ProtoBufDataColumn> Columns { get; set; } = new List<ProtoBufDataColumn>();

        public ProtoBufDataBuffer[] Buffers { get; set; }

        public bool ReachedEndOfCurrentResult { get; set; }

        public int CurrentFieldHeader { get; private set; }

        public int ReadFieldHeader()
        {
            this.CurrentFieldHeader = this.Reader.ReadFieldHeader();

            return this.CurrentFieldHeader;
        }

        public int ReadExpectedFieldHeader(int expectedFieldHeader)
        {
            this.CurrentFieldHeader = this.Reader.ReadFieldHeader();

            if (this.CurrentFieldHeader != expectedFieldHeader)
            {
                throw new InvalidDataException($"Field header '{expectedFieldHeader}' expected, actual '{this.CurrentFieldHeader}'.");
            }

            return this.CurrentFieldHeader;
        }

        public void StartSubItem()
        {
            this.subItemTokens.Push(ProtoReader.StartSubItem(this.Reader));
        }

        public void EndSubItem()
        {
            ProtoReader.EndSubItem(this.subItemTokens.Pop(), this.Reader);
        }
    }
}
