// Copyright 2011 Richard Dingwall - http://richarddingwall.name
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

using System;
using System.Data;
using System.IO;
using ProtoBuf.Data.Internal;

namespace ProtoBuf.Data
{

    public class ProtoDataWriter : IProtoDataWriter
    {
        public void Serialize(Stream stream, IDataReader reader)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (reader == null) throw new ArgumentNullException("reader");

            ProtoDataColumn[] cols;
            using (var schema = reader.GetSchemaTable())
            {
                cols = new ProtoDataColumn[schema.Rows.Count];
                for (var i = 0; i < schema.Rows.Count; i++)
                {
                    // Assumption: rows in the schema table are always ordered by
                    // Ordinal position, ascending
                    var row = schema.Rows[i];
                    cols[i].ProtoDataType = ConvertProtoDataType.FromClrType(row.Field<Type>("DataType"));
                    cols[i].ColumnName = schema.Rows[i].Field<string>("ColumnName");
                }
            }

            using (var writer = new ProtoWriter(stream, null, null))
            {
                // write the schema
                foreach (var col in cols)
                {
                    // for each, write the name and data type
                    ProtoWriter.WriteFieldHeader(2, WireType.StartGroup, writer);
                    var token = ProtoWriter.StartSubItem(col, writer);
                    ProtoWriter.WriteFieldHeader(1, WireType.String, writer);
                    ProtoWriter.WriteString(col.ColumnName, writer);
                    ProtoWriter.WriteFieldHeader(2, WireType.Variant, writer);
                    ProtoWriter.WriteInt32((int)col.ProtoDataType, writer);
                    ProtoWriter.EndSubItem(token, writer);
                }

                // write the rows
                var rowIndex = 0;
                while (reader.Read())
                {
                    var fieldIndex = 1;
                    ProtoWriter.WriteFieldHeader(3, WireType.StartGroup, writer);
                    var token = ProtoWriter.StartSubItem(rowIndex, writer);
                    foreach (var col in cols)
                    {
                        var value = reader[fieldIndex - 1];
                        if (value == null || value is DBNull || IsZeroLengthArray(value))
                        {
                            // don't write anything
                        }
                        else
                        {
                            switch (col.ProtoDataType)
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
                                    ProtoWriter.WriteInt16((Int16)value, writer);
                                    break;

                                case ProtoDataType.Double:
                                    ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Variant, writer);
                                    ProtoWriter.WriteDouble((double)value, writer);
                                    break;

                                case ProtoDataType.Float:
                                    ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Variant, writer);
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
                                default:
                                    throw new UnsupportedColumnTypeException(ConvertProtoDataType.ToClrType(col.ProtoDataType));
                            }
                            
                        }
                        fieldIndex++;
                    }
                    ProtoWriter.EndSubItem(token, writer);
                    rowIndex++;
                }
            }
        }

        private static bool IsZeroLengthArray(object value)
        {
            var array = value as Array;

            if (array == null)
                return false;

            return array.Length == 0;
        }
    }
}