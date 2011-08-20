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
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using ProtoBuf.Data.Internal;

namespace ProtoBuf.Data
{

    public class ProtoDataWriter : IProtoDataWriter
    {
        public void Serialize(Stream stream, IDataReader reader)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (reader == null) throw new ArgumentNullException("reader");

            IList<ProtoDataColumn> cols;
            using (var schema = reader.GetSchemaTable())
            {
                cols = (from col in schema.AsEnumerable()
                           let name = col.Field<string>("ColumnName")
                           let ordinal = col.Field<int>("ColumnOrdinal")
                           let type = ConvertProtoDataType.FromClrType(col.Field<Type>("DataType"))
                           select new ProtoDataColumn {ColumnName = name, ProtoDataType = type, Ordinal = ordinal}
                           ).ToList();
            }

            using (var writer = new ProtoWriter(stream, null, null))
            {
                // write the schema:
                var colWriters = new Action<object>[cols.Count];
                int i = 0;

                foreach (ProtoDataColumn col in cols)
                {
                    // for each, write the name and data type
                    ProtoWriter.WriteFieldHeader(2, WireType.StartGroup, writer);
                    var token = ProtoWriter.StartSubItem(col, writer);
                    ProtoWriter.WriteFieldHeader(1, WireType.String, writer);
                    ProtoWriter.WriteString(col.ColumnName, writer);
                    ProtoWriter.WriteFieldHeader(2, WireType.Variant, writer);


                    ProtoWriter.WriteInt32((int) col.ProtoDataType, writer);
                    ProtoWriter.EndSubItem(token, writer);
                    int field = i + 1;
                    Action<object> colWriter;
                    switch (col.ProtoDataType)
                    {
                        case ProtoDataType.String:
                            colWriter = value =>
                                            {
                                                ProtoWriter.WriteFieldHeader(field, WireType.String, writer);
                                                ProtoWriter.WriteString((string) value, writer);
                                            };
                            break;
                        case ProtoDataType.Short:
                            colWriter = value =>
                                            {
                                                ProtoWriter.WriteFieldHeader(field, WireType.Variant, writer);
                                                ProtoWriter.WriteInt16((short) value, writer);
                                            };
                            break;
                        case ProtoDataType.Decimal:
                            colWriter = value =>
                                            {
                                                ProtoWriter.WriteFieldHeader(field, WireType.StartGroup, writer);
                                                BclHelpers.WriteDecimal((decimal) value, writer);
                                            };
                            break;
                        case ProtoDataType.Int:
                            colWriter = value =>
                                            {
                                                ProtoWriter.WriteFieldHeader(field, WireType.Variant, writer);
                                                ProtoWriter.WriteInt32((int) value, writer);
                                            };
                            break;
                        case ProtoDataType.Guid:
                            colWriter = value =>
                                            {
                                                ProtoWriter.WriteFieldHeader(field, WireType.StartGroup, writer);
                                                BclHelpers.WriteGuid((Guid) value, writer);
                                            };
                            break;
                        case ProtoDataType.DateTime:
                            colWriter = value =>
                                            {
                                                ProtoWriter.WriteFieldHeader(field, WireType.StartGroup, writer);
                                                BclHelpers.WriteDateTime((DateTime) value, writer);
                                            };
                            break;

                        case ProtoDataType.Bool:
                            colWriter = value =>
                                            {
                                                ProtoWriter.WriteFieldHeader(field, WireType.StartGroup, writer);
                                                ProtoWriter.WriteBoolean((bool) value, writer);
                                            };
                            break;
                        case ProtoDataType.Byte:
                            colWriter = value =>
                                            {
                                                ProtoWriter.WriteFieldHeader(field, WireType.StartGroup, writer);
                                                ProtoWriter.WriteByte((byte) value, writer);
                                            };
                            break;
                        case ProtoDataType.Char:
                            colWriter = value =>
                                            {
                                                ProtoWriter.WriteFieldHeader(field, WireType.StartGroup, writer);
                                                ProtoWriter.WriteInt16((Int16) value, writer);
                                            };
                            break;
                        case ProtoDataType.Double:
                            colWriter = value =>
                                            {
                                                ProtoWriter.WriteFieldHeader(field, WireType.StartGroup, writer);
                                                ProtoWriter.WriteDouble((double) value, writer);
                                            };
                            break;
                        case ProtoDataType.Float:
                            colWriter = value =>
                                            {
                                                ProtoWriter.WriteFieldHeader(field, WireType.StartGroup, writer);
                                                ProtoWriter.WriteSingle((float) value, writer);
                                            };
                            break;
                        case ProtoDataType.Long:
                            colWriter = value =>
                                            {
                                                ProtoWriter.WriteFieldHeader(field, WireType.StartGroup, writer);
                                                ProtoWriter.WriteInt64((long) value, writer);
                                            };
                            break;

                        default:
                            throw new NotSupportedException(col.ProtoDataType.ToString());
                    }
                    colWriters[i++] = colWriter;
                }

                // write the rows
                while (reader.Read())
                {
                    i = 0;
                    ProtoWriter.WriteFieldHeader(3, WireType.StartGroup, writer);
                    var token = ProtoWriter.StartSubItem(i, writer);
                    foreach (var col in cols)
                    {
                        var value = reader[col.Ordinal];
                        if (value == null || value is DBNull)
                        {
                        }
                        else
                        {
                            colWriters[i](value);
                        }
                        i++;
                    }
                    ProtoWriter.EndSubItem(token, writer);
                }
            }
        }
    }
}