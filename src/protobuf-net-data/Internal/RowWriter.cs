// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;

namespace ProtoBuf.Data.Internal
{
    internal sealed class RowWriter
    {
        private readonly ProtoWriter writer;
        private readonly IEnumerable<ProtoDataColumn> columns;
        private readonly ProtoDataWriterOptions options;
        private int rowIndex;

        public RowWriter(
            ProtoWriter writer,
            IEnumerable<ProtoDataColumn> columns,
            ProtoDataWriterOptions options)
        {
            if (writer == null)
            {
                throw new ArgumentNullException("writer");
            }

            if (columns == null)
            {
                throw new ArgumentNullException("columns");
            }

            if (options == null)
            {
                throw new ArgumentNullException("options");
            }

            this.writer = writer;
            this.columns = columns;
            this.options = options;
            this.rowIndex = 0;
        }

        public void WriteRow(IDataRecord row)
        {
            int fieldIndex = 1;
            int columnIndex = 0;
            ProtoWriter.WriteFieldHeader(3, WireType.StartGroup, this.writer);
            SubItemToken token = ProtoWriter.StartSubItem(this.rowIndex, this.writer);

            foreach (ProtoDataColumn column in this.columns)
            {
                object value = row[columnIndex];
                if (value == null || value is DBNull || (this.options.SerializeEmptyArraysAsNull && IsZeroLengthArray(value)))
                {
                    // don't write anything
                }
                else
                {
                    switch (column.ProtoDataType)
                    {
                        case ProtoDataType.String:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.String, this.writer);
                            ProtoWriter.WriteString((string)value, this.writer);
                            break;

                        case ProtoDataType.Short:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Variant, this.writer);
                            ProtoWriter.WriteInt16((short)value, this.writer);
                            break;

                        case ProtoDataType.Decimal:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.StartGroup, this.writer);
                            BclHelpers.WriteDecimal((decimal)value, this.writer);
                            break;

                        case ProtoDataType.Int:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Variant, this.writer);
                            ProtoWriter.WriteInt32((int)value, this.writer);
                            break;

                        case ProtoDataType.Guid:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.StartGroup, this.writer);
                            BclHelpers.WriteGuid((Guid)value, this.writer);
                            break;

                        case ProtoDataType.DateTime:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.StartGroup, this.writer);
                            BclHelpers.WriteDateTime((DateTime)value, this.writer);
                            break;

                        case ProtoDataType.Bool:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Variant, this.writer);
                            ProtoWriter.WriteBoolean((bool)value, this.writer);
                            break;

                        case ProtoDataType.Byte:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Variant, this.writer);
                            ProtoWriter.WriteByte((byte)value, this.writer);
                            break;

                        case ProtoDataType.Char:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Variant, this.writer);
                            ProtoWriter.WriteInt16((short)(char)value, this.writer);
                            break;

                        case ProtoDataType.Double:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Fixed64, this.writer);
                            ProtoWriter.WriteDouble((double)value, this.writer);
                            break;

                        case ProtoDataType.Float:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Fixed32, this.writer);
                            ProtoWriter.WriteSingle((float)value, this.writer);
                            break;

                        case ProtoDataType.Long:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Variant, this.writer);
                            ProtoWriter.WriteInt64((long)value, this.writer);
                            break;

                        case ProtoDataType.ByteArray:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.String, this.writer);
                            ProtoWriter.WriteBytes((byte[])value, 0, ((byte[])value).Length, this.writer);
                            break;

                        case ProtoDataType.CharArray:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.String, this.writer);
                            ProtoWriter.WriteString(new string((char[])value), this.writer);
                            break;

                        case ProtoDataType.TimeSpan:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.StartGroup, this.writer);
                            BclHelpers.WriteTimeSpan((TimeSpan)value, this.writer);
                            break;

                        default:
                            throw new UnsupportedColumnTypeException(
                                ConvertProtoDataType.ToClrType(column.ProtoDataType));
                    }
                }

                fieldIndex++;
                columnIndex++;
            }

            ProtoWriter.EndSubItem(token, this.writer);
            this.rowIndex++;
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