// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Data;

namespace ProtoBuf.Data.Internal
{
    internal static class RecordWriter
    {
        private const int RecordFieldHeader = 3;

        public static void WriteRecord(ProtoWriterContext context, int recordIndex, IDataRecord record)
        {
            ProtoWriter.WriteFieldHeader(RecordFieldHeader, WireType.StartGroup, context.Writer);

            context.StartSubItem(recordIndex);

            for (var columnIndex = 0; columnIndex < context.Columns.Count; columnIndex++)
            {
                // The check whether record[columnIndex] == null is removed due to the fact that value types have to be
                // boxed into an object just to check whether the field value is null. This creates garbage, thus has
                // impact on garbage collection. Removing the check should not be a problem as IDataRecord.IsDBNull
                // should be sufficient (tested with SqlDataReader, SqlLiteDataReader, and DataTableReader).
                if (record.IsDBNull(columnIndex))
                {
                    continue;
                }

                var fieldNumber = columnIndex + 1;

                switch (context.Columns[columnIndex].ProtoDataType)
                {
                    case ProtoDataType.String:
                        ProtoWriter.WriteFieldHeader(fieldNumber, WireType.String, context.Writer);
                        ProtoWriter.WriteString(record.GetString(columnIndex), context.Writer);
                        break;

                    case ProtoDataType.Short:
                        ProtoWriter.WriteFieldHeader(fieldNumber, WireType.Variant, context.Writer);
                        ProtoWriter.WriteInt16(record.GetInt16(columnIndex), context.Writer);
                        break;

                    case ProtoDataType.Decimal:
                        ProtoWriter.WriteFieldHeader(fieldNumber, WireType.StartGroup, context.Writer);
                        BclHelpers.WriteDecimal(record.GetDecimal(columnIndex), context.Writer);
                        break;

                    case ProtoDataType.Int:
                        ProtoWriter.WriteFieldHeader(fieldNumber, WireType.Variant, context.Writer);
                        ProtoWriter.WriteInt32(record.GetInt32(columnIndex), context.Writer);
                        break;

                    case ProtoDataType.Guid:
                        ProtoWriter.WriteFieldHeader(fieldNumber, WireType.StartGroup, context.Writer);
                        BclHelpers.WriteGuid(record.GetGuid(columnIndex), context.Writer);
                        break;

                    case ProtoDataType.DateTime:
                        ProtoWriter.WriteFieldHeader(fieldNumber, WireType.StartGroup, context.Writer);
                        BclHelpers.WriteDateTime(record.GetDateTime(columnIndex), context.Writer);
                        break;

                    case ProtoDataType.Bool:
                        ProtoWriter.WriteFieldHeader(fieldNumber, WireType.Variant, context.Writer);
                        ProtoWriter.WriteBoolean(record.GetBoolean(columnIndex), context.Writer);
                        break;

                    case ProtoDataType.Byte:
                        ProtoWriter.WriteFieldHeader(fieldNumber, WireType.Variant, context.Writer);
                        ProtoWriter.WriteByte(record.GetByte(columnIndex), context.Writer);
                        break;

                    case ProtoDataType.Char:
                        ProtoWriter.WriteFieldHeader(fieldNumber, WireType.Variant, context.Writer);
                        ProtoWriter.WriteInt16((short)record.GetChar(columnIndex), context.Writer);
                        break;

                    case ProtoDataType.Double:
                        ProtoWriter.WriteFieldHeader(fieldNumber, WireType.Fixed64, context.Writer);
                        ProtoWriter.WriteDouble(record.GetDouble(columnIndex), context.Writer);
                        break;

                    case ProtoDataType.Float:
                        ProtoWriter.WriteFieldHeader(fieldNumber, WireType.Fixed32, context.Writer);
                        ProtoWriter.WriteSingle(record.GetFloat(columnIndex), context.Writer);
                        break;

                    case ProtoDataType.Long:
                        ProtoWriter.WriteFieldHeader(fieldNumber, WireType.Variant, context.Writer);
                        ProtoWriter.WriteInt64(record.GetInt64(columnIndex), context.Writer);
                        break;

                    case ProtoDataType.ByteArray:
                        var bytes = (byte[])record[columnIndex];

                        if (bytes.Length != 0 || !context.Options.SerializeEmptyArraysAsNull)
                        {
                            ProtoWriter.WriteFieldHeader(fieldNumber, WireType.String, context.Writer);
                            ProtoWriter.WriteBytes(bytes, 0, bytes.Length, context.Writer);
                        }

                        break;

                    case ProtoDataType.CharArray:
                        var characters = (char[])record[columnIndex];

                        if (characters.Length != 0 || !context.Options.SerializeEmptyArraysAsNull)
                        {
                            ProtoWriter.WriteFieldHeader(fieldNumber, WireType.String, context.Writer);
                            ProtoWriter.WriteString(new string(characters), context.Writer);
                        }

                        break;

                    case ProtoDataType.TimeSpan:
                        ProtoWriter.WriteFieldHeader(fieldNumber, WireType.StartGroup, context.Writer);
                        BclHelpers.WriteTimeSpan((TimeSpan)record[columnIndex], context.Writer);
                        break;

                    default:
                        throw new UnsupportedColumnTypeException(ConvertProtoDataType.ToClrType(context.Columns[columnIndex].ProtoDataType));
                }
            }

            context.EndSubItem();
        }
    }
}
