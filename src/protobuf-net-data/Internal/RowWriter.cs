// Copyright 2012 Richard Dingwall - http://richarddingwall.name
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace ProtoBuf.Data.Internal
{
    using System;
    using System.Collections.Generic;
    using System.Data;

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
            rowIndex = 0;
        }

        public void WriteRow(IDataRecord row)
        {
            int fieldIndex = 1;
            ProtoWriter.WriteFieldHeader(3, WireType.StartGroup, writer);
            SubItemToken token = ProtoWriter.StartSubItem(rowIndex, writer);

            foreach (ProtoDataColumn column in columns)
            {
                object value = row[column.ColumnIndex];
                if (value == null || value is DBNull || (options.SerializeEmptyArraysAsNull && IsZeroLengthArray(value)))
                {
                    // don't write anything
                }
                else
                {
                    switch (column.ProtoDataType)
                    {
                        case ProtoDataType.String:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.String, writer);
                            ProtoWriter.WriteString((string)value, writer);
                            break;

                        case ProtoDataType.Short:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Variant, writer);
                            ProtoWriter.WriteInt16((short)value, writer);
                            break;

                        case ProtoDataType.Decimal:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.StartGroup, writer);
                            BclHelpers.WriteDecimal((decimal)value, writer);
                            break;

                        case ProtoDataType.Int:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Variant, writer);
                            ProtoWriter.WriteInt32((int)value, writer);
                            break;

                        case ProtoDataType.Guid:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.StartGroup, writer);
                            BclHelpers.WriteGuid((Guid)value, writer);
                            break;

                        case ProtoDataType.DateTime:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.StartGroup, writer);
                            BclHelpers.WriteDateTime((DateTime)value, writer);
                            break;

                        case ProtoDataType.Bool:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Variant, writer);
                            ProtoWriter.WriteBoolean((bool)value, writer);
                            break;

                        case ProtoDataType.Byte:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Variant, writer);
                            ProtoWriter.WriteByte((byte)value, writer);
                            break;

                        case ProtoDataType.Char:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Variant, writer);
                            ProtoWriter.WriteInt16((short)(char)value, writer);
                            break;

                        case ProtoDataType.Double:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Fixed64, writer);
                            ProtoWriter.WriteDouble((double)value, writer);
                            break;

                        case ProtoDataType.Float:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Fixed32, writer);
                            ProtoWriter.WriteSingle((float)value, writer);
                            break;

                        case ProtoDataType.Long:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Variant, writer);
                            ProtoWriter.WriteInt64((long)value, writer);
                            break;

                        case ProtoDataType.ByteArray:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.String, writer);
                            ProtoWriter.WriteBytes((byte[])value, 0, ((byte[])value).Length, writer);
                            break;

                        case ProtoDataType.CharArray:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.String, writer);
                            ProtoWriter.WriteString(new string((char[])value), writer);
                            break;

                        case ProtoDataType.TimeSpan:
                            ProtoWriter.WriteFieldHeader(fieldIndex, WireType.StartGroup, writer);
                            BclHelpers.WriteTimeSpan((TimeSpan)value, writer);
                            break;

                        default:
                            throw new UnsupportedColumnTypeException(
                                ConvertProtoDataType.ToClrType(column.ProtoDataType));
                    }
                }

                fieldIndex++;
            }

            ProtoWriter.EndSubItem(token, writer);
            rowIndex++;
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