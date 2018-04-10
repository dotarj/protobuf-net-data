// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;

namespace ProtoBuf.Data.Internal
{
    internal sealed class HeaderWriter
    {
        private readonly ProtoWriter writer;

        public HeaderWriter(ProtoWriter writer)
        {
            if (writer == null)
            {
                throw new ArgumentNullException("writer");
            }

            this.writer = writer;
        }

        public void WriteHeader(IEnumerable<ProtoDataColumn> columns)
        {
            if (columns == null)
            {
                throw new ArgumentNullException("columns");
            }

            foreach (ProtoDataColumn column in columns)
            {
                // for each, write the name and data type
                ProtoWriter.WriteFieldHeader(2, WireType.StartGroup, this.writer);
                SubItemToken columnToken = ProtoWriter.StartSubItem(column, this.writer);
                ProtoWriter.WriteFieldHeader(1, WireType.String, this.writer);
                ProtoWriter.WriteString(column.Name, this.writer);
                ProtoWriter.WriteFieldHeader(2, WireType.Variant, this.writer);
                ProtoWriter.WriteInt32((int)column.ProtoDataType, this.writer);
                ProtoWriter.EndSubItem(columnToken, this.writer);
            }
        }
    }
}