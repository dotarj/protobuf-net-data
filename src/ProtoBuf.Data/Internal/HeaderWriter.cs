using System;
using System.Collections.Generic;

namespace ProtoBuf.Data.Internal
{
    internal class HeaderWriter
    {
        private readonly ProtoWriter writer;

        public HeaderWriter(ProtoWriter writer)
        {
            if (writer == null) throw new ArgumentNullException("writer");
            this.writer = writer;
        }

        public void WriteHeader(IEnumerable<ProtoDataColumn> columns)
        {
            if (columns == null) throw new ArgumentNullException("columns");

            foreach (var column in columns)
            {
                // for each, write the name and data type
                ProtoWriter.WriteFieldHeader(2, WireType.StartGroup, writer);
                var columnToken = ProtoWriter.StartSubItem(column, writer);
                ProtoWriter.WriteFieldHeader(1, WireType.String, writer);
                ProtoWriter.WriteString(column.ColumnName, writer);
                ProtoWriter.WriteFieldHeader(2, WireType.Variant, writer);
                ProtoWriter.WriteInt32((int)column.ProtoDataType, writer);
                ProtoWriter.EndSubItem(columnToken, writer);
            }
        }
    }
}