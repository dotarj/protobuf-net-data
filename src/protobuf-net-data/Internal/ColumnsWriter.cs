// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

namespace ProtoBuf.Data.Internal
{
    internal static class ColumnsWriter
    {
        private const int ColumnFieldHeader = 2;
        private const int ColumnNameFieldHeader = 1;
        private const int ColumnTypeFieldHeader = 2;

        public static void WriteColumns(ProtoWriterContext context)
        {
            foreach (var column in context.Columns)
            {
                ProtoWriter.WriteFieldHeader(ColumnFieldHeader, WireType.StartGroup, context.Writer);

                context.StartSubItem(column);

                WriteColumnName(context, column);
                WriteColumnType(context, column);

                context.EndSubItem();
            }
        }

        private static void WriteColumnName(ProtoWriterContext context, ProtoDataColumn column)
        {
            ProtoWriter.WriteFieldHeader(ColumnNameFieldHeader, WireType.String, context.Writer);
            ProtoWriter.WriteString(column.Name, context.Writer);
        }

        private static void WriteColumnType(ProtoWriterContext context, ProtoDataColumn column)
        {
            ProtoWriter.WriteFieldHeader(ColumnTypeFieldHeader, WireType.Variant, context.Writer);
            ProtoWriter.WriteInt32((int)column.ProtoDataType, context.Writer);
        }
    }
}