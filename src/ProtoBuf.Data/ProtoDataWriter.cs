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
using System.Collections.Generic;
using System.Data;
using System.IO;
using ProtoBuf.Data.Internal;

namespace ProtoBuf.Data
{
    ///<summary>
    /// Serializes an <see cref="System.Data.IDataReader"/> to a binary stream.
    ///</summary>
    public class ProtoDataWriter : IProtoDataWriter
    {
        ///<summary>
        /// Serialize an <see cref="System.Data.IDataReader"/> to a binary stream using protocol-buffers.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        ///<param name="reader">The <see cref="System.Data.IDataReader"/>who's contents to serialize.</param>
        public void Serialize(Stream stream, IDataReader reader)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (reader == null) throw new ArgumentNullException("reader");

            int resultIndex = 0;

            using (var writer = new ProtoWriter(stream, null, null))
            {
                do
                {
                    IList<ProtoDataColumn> cols;
                    using (var schema = reader.GetSchemaTable())
                    {
                        bool schemaSupportsExpressions = schema.Columns.Contains("Expression");

                        cols = new List<ProtoDataColumn>(schema.Rows.Count);
                        for (var i = 0; i < schema.Rows.Count; i++)
                        {
                            // Assumption: rows in the schema table are always ordered by
                            // Ordinal position, ascending
                            var row = schema.Rows[i];

                            // Skip computed columns. No point serializing and transmitting
                            // these - just redeclare them after deserializing.
                            if (schemaSupportsExpressions && !(row["Expression"] is DBNull))
                                continue;

                            var col = new ProtoDataColumn
                                          {
                                              ColumnIndex = i,
                                              ProtoDataType = ConvertProtoDataType.FromClrType((Type) row["DataType"]),
                                              ColumnName = (string) row["ColumnName"]
                                          };

                            cols.Add(col);
                        }
                    }

                    // This is the underlying protocol buffers structure we use:
                    //
                    // <1 StartGroup> each DataTable
                    // <SubItem>
                    //     <2 StartGroup> each DataColumn
                    //     <SubItem>
                    //         <1 String> Column Name
                    //         <2 Variant> Column ProtoDataType (enum casted to int)
                    //     </SubItem>
                    //     <3 StartGroup> each DataRow
                    //     <SubItem>
                    //         <(# Column Index) (corresponding type)> Field Value
                    //     </SubItem>
                    // </SubItem>
                    //
                    // NB if Field Value is a DataTable, the whole DataTable is 

                    // write the table
                    ProtoWriter.WriteFieldHeader(1, WireType.StartGroup, writer);
                    var resultToken = ProtoWriter.StartSubItem(resultIndex, writer);

                    // write the schema
                    foreach (var col in cols)
                    {
                        // for each, write the name and data type
                        ProtoWriter.WriteFieldHeader(2, WireType.StartGroup, writer);
                        var columnToken = ProtoWriter.StartSubItem(col, writer);
                        ProtoWriter.WriteFieldHeader(1, WireType.String, writer);
                        ProtoWriter.WriteString(col.ColumnName, writer);
                        ProtoWriter.WriteFieldHeader(2, WireType.Variant, writer);
                        ProtoWriter.WriteInt32((int) col.ProtoDataType, writer);
                        ProtoWriter.EndSubItem(columnToken, writer);
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
                            var value = reader[col.ColumnIndex];
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
                                        ProtoWriter.WriteString((string) value, writer);
                                        break;

                                    case ProtoDataType.Short:
                                        ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Variant, writer);
                                        ProtoWriter.WriteInt16((short) value, writer);
                                        break;

                                    case ProtoDataType.Decimal:
                                        ProtoWriter.WriteFieldHeader(fieldIndex, WireType.StartGroup, writer);
                                        BclHelpers.WriteDecimal((decimal) value, writer);
                                        break;

                                    case ProtoDataType.Int:
                                        ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Variant, writer);
                                        ProtoWriter.WriteInt32((int) value, writer);
                                        break;

                                    case ProtoDataType.Guid:
                                        ProtoWriter.WriteFieldHeader(fieldIndex, WireType.StartGroup, writer);
                                        BclHelpers.WriteGuid((Guid) value, writer);
                                        break;

                                    case ProtoDataType.DateTime:
                                        ProtoWriter.WriteFieldHeader(fieldIndex, WireType.StartGroup, writer);
                                        BclHelpers.WriteDateTime((DateTime) value, writer);
                                        break;

                                    case ProtoDataType.Bool:
                                        ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Variant, writer);
                                        ProtoWriter.WriteBoolean((bool) value, writer);
                                        break;

                                    case ProtoDataType.Byte:
                                        ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Variant, writer);
                                        ProtoWriter.WriteByte((byte) value, writer);
                                        break;

                                    case ProtoDataType.Char:
                                        ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Variant, writer);
                                        ProtoWriter.WriteInt16((Int16)(char) value, writer);
                                        break;

                                    case ProtoDataType.Double:
                                        ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Fixed64, writer);
                                        ProtoWriter.WriteDouble((double) value, writer);
                                        break;

                                    case ProtoDataType.Float:
                                        ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Fixed32, writer);
                                        ProtoWriter.WriteSingle((float) value, writer);
                                        break;

                                    case ProtoDataType.Long:
                                        ProtoWriter.WriteFieldHeader(fieldIndex, WireType.Variant, writer);
                                        ProtoWriter.WriteInt64((long) value, writer);
                                        break;

                                    case ProtoDataType.ByteArray:
                                        ProtoWriter.WriteFieldHeader(fieldIndex, WireType.String, writer);
                                        ProtoWriter.WriteBytes((byte[]) value, 0, ((byte[]) value).Length, writer);
                                        break;

                                    case ProtoDataType.CharArray:
                                        ProtoWriter.WriteFieldHeader(fieldIndex, WireType.String, writer);
                                        ProtoWriter.WriteString(new string((char[]) value), writer);
                                        break;

                                    case ProtoDataType.DataTable:
                                        ProtoWriter.WriteFieldHeader(fieldIndex, WireType.String, writer);
                                        WriteNestedDataTableBytes(writer, value);
                                        break;

                                    default:
                                        throw new UnsupportedColumnTypeException(
                                            ConvertProtoDataType.ToClrType(col.ProtoDataType));
                                }

                            }
                            fieldIndex++;
                        }
                        ProtoWriter.EndSubItem(token, writer);
                        rowIndex++;
                    }

                    ProtoWriter.EndSubItem(resultToken, writer);

                } while (reader.NextResult());
            }
        }

        private static void WriteNestedDataTableBytes(ProtoWriter writer, object value)
        {
            var nestedDataTable = (DataTable) value;

            using (var nestedDataReader = nestedDataTable.CreateDataReader())
            using (var nestedBuffer = new MemoryStream())
            {
                new ProtoDataWriter().Serialize(nestedBuffer, nestedDataReader);
                ProtoWriter.WriteBytes(nestedBuffer.GetBuffer(), writer);
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