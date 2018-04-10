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
                var value = record[columnIndex];

                if (value == null || value is DBNull || (context.Options.SerializeEmptyArraysAsNull && IsZeroLengthArray(value)))
                {
                    // don't write anything
                }
                else
                {
                    var fieldNumber = columnIndex + 1;

                    switch (context.Columns[columnIndex].ProtoDataType)
                    {
                        case ProtoDataType.String:
                            ProtoWriter.WriteFieldHeader(fieldNumber, WireType.String, context.Writer);
                            ProtoWriter.WriteString((string)value, context.Writer);
                            break;

                        case ProtoDataType.Short:
                            ProtoWriter.WriteFieldHeader(fieldNumber, WireType.Variant, context.Writer);
                            ProtoWriter.WriteInt16((short)value, context.Writer);
                            break;

                        case ProtoDataType.Decimal:
                            ProtoWriter.WriteFieldHeader(fieldNumber, WireType.StartGroup, context.Writer);
                            BclHelpers.WriteDecimal((decimal)value, context.Writer);
                            break;

                        case ProtoDataType.Int:
                            ProtoWriter.WriteFieldHeader(fieldNumber, WireType.Variant, context.Writer);
                            ProtoWriter.WriteInt32((int)value, context.Writer);
                            break;

                        case ProtoDataType.Guid:
                            ProtoWriter.WriteFieldHeader(fieldNumber, WireType.StartGroup, context.Writer);
                            BclHelpers.WriteGuid((Guid)value, context.Writer);
                            break;

                        case ProtoDataType.DateTime:
                            ProtoWriter.WriteFieldHeader(fieldNumber, WireType.StartGroup, context.Writer);
                            BclHelpers.WriteDateTime((DateTime)value, context.Writer);
                            break;

                        case ProtoDataType.Bool:
                            ProtoWriter.WriteFieldHeader(fieldNumber, WireType.Variant, context.Writer);
                            ProtoWriter.WriteBoolean((bool)value, context.Writer);
                            break;

                        case ProtoDataType.Byte:
                            ProtoWriter.WriteFieldHeader(fieldNumber, WireType.Variant, context.Writer);
                            ProtoWriter.WriteByte((byte)value, context.Writer);
                            break;

                        case ProtoDataType.Char:
                            ProtoWriter.WriteFieldHeader(fieldNumber, WireType.Variant, context.Writer);
                            ProtoWriter.WriteInt16((short)(char)value, context.Writer);
                            break;

                        case ProtoDataType.Double:
                            ProtoWriter.WriteFieldHeader(fieldNumber, WireType.Fixed64, context.Writer);
                            ProtoWriter.WriteDouble((double)value, context.Writer);
                            break;

                        case ProtoDataType.Float:
                            ProtoWriter.WriteFieldHeader(fieldNumber, WireType.Fixed32, context.Writer);
                            ProtoWriter.WriteSingle((float)value, context.Writer);
                            break;

                        case ProtoDataType.Long:
                            ProtoWriter.WriteFieldHeader(fieldNumber, WireType.Variant, context.Writer);
                            ProtoWriter.WriteInt64((long)value, context.Writer);
                            break;

                        case ProtoDataType.ByteArray:
                            ProtoWriter.WriteFieldHeader(fieldNumber, WireType.String, context.Writer);
                            ProtoWriter.WriteBytes((byte[])value, 0, ((byte[])value).Length, context.Writer);
                            break;

                        case ProtoDataType.CharArray:
                            ProtoWriter.WriteFieldHeader(fieldNumber, WireType.String, context.Writer);
                            ProtoWriter.WriteString(new string((char[])value), context.Writer);
                            break;

                        case ProtoDataType.TimeSpan:
                            ProtoWriter.WriteFieldHeader(fieldNumber, WireType.StartGroup, context.Writer);
                            BclHelpers.WriteTimeSpan((TimeSpan)value, context.Writer);
                            break;

                        default:
                            throw new UnsupportedColumnTypeException(ConvertProtoDataType.ToClrType(context.Columns[columnIndex].ProtoDataType));
                    }
                }
            }

            context.EndSubItem();
        }

        private static bool IsZeroLengthArray(object value)
        {
            var array = value as Array;

            if (array == null)
            {
                return false;
            }

            return array.Length == 0;
        }
    }
}
