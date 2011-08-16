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

            var header = GetHeader(reader);

            Serializer.SerializeWithLengthPrefix(stream, header, PrefixStyle.Fixed32);

            var rowFactory = new ProtoDataRowFactory(header);

            while (reader.Read())
            {
                var itemArray = new object[header.Columns.Count];
                reader.GetValues(itemArray);

                var row = rowFactory.CreateRow();
                PopulateValues(row, header, reader);

                Serializer.SerializeWithLengthPrefix(stream, row, PrefixStyle.Fixed32);
            }
        }

        static void PopulateValues(ProtoDataRow row, ProtoDataHeader header, IDataRecord reader)
        {
            var values = new object[header.Columns.Count];
            reader.GetValues(values);

            foreach (var column in header.Columns)
            {
                if (values[column.Ordinal] == DBNull.Value)
                {
                    row.NullColumns[column.Ordinal] = true;
                    continue;
                }

                switch (column.ProtoDataType)
                {
                    case ProtoDataType.Bool:
                        row.BoolValues[column.OrdinalWithinType] = (bool)values[column.Ordinal];
                        break;

                    case ProtoDataType.Byte:
                        row.ByteValues[column.OrdinalWithinType] = (byte)values[column.Ordinal];
                        break;

                    case ProtoDataType.DateTime:
                        row.DateTimeValues[column.OrdinalWithinType] = (DateTime)values[column.Ordinal];
                        break;

                    case ProtoDataType.Double:
                        row.DoubleValues[column.OrdinalWithinType] = (double)values[column.Ordinal];
                        break;

                    case ProtoDataType.Float:
                        row.FloatValues[column.OrdinalWithinType] = (float)values[column.Ordinal];
                        break;

                    case ProtoDataType.Int:
                        row.Int32Values[column.OrdinalWithinType] = (int)values[column.Ordinal];
                        break;

                    case ProtoDataType.Long:
                        row.Int64Values[column.OrdinalWithinType] = (long)values[column.Ordinal];
                        break;

                    case ProtoDataType.Short:
                        row.Int16Values[column.OrdinalWithinType] = (short)values[column.Ordinal];
                        break;

                    case ProtoDataType.Char:
                        row.CharValues[column.OrdinalWithinType] = (char)values[column.Ordinal];
                        break;

                    case ProtoDataType.String:
                        row.StringValues[column.OrdinalWithinType] = (string)values[column.Ordinal];
                        break;

                    case ProtoDataType.Decimal:
                        row.DecimalValues[column.OrdinalWithinType] = (decimal)values[column.Ordinal];
                        break;

                    case ProtoDataType.Guid:
                        row.GuidValues[column.OrdinalWithinType] = (Guid)values[column.Ordinal];
                        break;
                }
            }
        }

        static ProtoDataHeader GetHeader(IDataReader reader)
        {
            using (var schema = reader.GetSchemaTable())
            {
                var columns = schema.Rows
                    .OfType<DataRow>()
                    .Select(r => new ProtoDataColumn
                                     {
                                         ColumnName = r.Field<string>("ColumnName"),
                                         ProtoDataType = ConvertProtoDataType.FromClrType(r.Field<Type>("DataType")),
                                         Ordinal = r.Field<int>("ColumnOrdinal")
                                     })
                                     .ToList();

                foreach (var dataType in ProtoDataTypes.AllTypes)
                {
                    var cols = columns.Where(c => dataType.Equals(c.ProtoDataType)).ToList();

                    for (int i = 0; i < cols.Count; i++)
                        cols[i].OrdinalWithinType = i;
                }

                return new ProtoDataHeader
                           {
                               Columns = columns.ToList()
                           };
            }
        }
    }
}