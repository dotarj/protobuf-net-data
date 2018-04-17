// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System.Collections.Generic;

namespace ProtoBuf.Data.Internal
{
    internal class ProtoWriterContext
    {
        private readonly Stack<SubItemToken> subItemTokens = new Stack<SubItemToken>();

        public ProtoWriterContext(ProtoWriter writer, ProtoDataWriterOptions options)
        {
            this.Writer = writer;
            this.Options = options;
        }

        public ProtoWriter Writer { get; }

        public ProtoDataWriterOptions Options { get; }

        public IList<ProtoDataColumn> Columns { get; set; } = new List<ProtoDataColumn>();

        public void StartSubItem(object instance)
        {
            this.subItemTokens.Push(ProtoWriter.StartSubItem(instance, this.Writer));
        }

        public void EndSubItem()
        {
            ProtoWriter.EndSubItem(this.subItemTokens.Pop(), this.Writer);
        }
    }
}
