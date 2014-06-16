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

using System;
using System.Collections.Generic;
using System.Data;

namespace ProtoBuf.Data.Internal
{
    internal class ProtoDataColumnFactory
    {
        private static readonly bool isRunningOnMono;

        static ProtoDataColumnFactory()
        {
            // From http://stackoverflow.com/a/721194/91551
            isRunningOnMono = Type.GetType("Mono.Runtime") != null;
        }

        public IEnumerable<ProtoDataColumn> GetColumns(
            IDataReader reader, 
            ProtoDataWriterOptions options)
        {
            if (reader == null) throw new ArgumentNullException("reader");
            if (options == null) throw new ArgumentNullException("options");

            using (DataTable schema = reader.GetSchemaTable())
            {
                bool schemaSupportsExpressions = schema.Columns.Contains("Expression");

                var columns = new List<ProtoDataColumn>(schema.Rows.Count);
                for (int i = 0; i < schema.Rows.Count; i++)
                {
                    // Assumption: rows in the schema table are always ordered by
                    // Ordinal position, ascending
                    DataRow row = schema.Rows[i];

                    // Skip computed columns unless requested.
                    if (schemaSupportsExpressions)
                    {
                        bool isComputedColumn;

                        if (isRunningOnMono)
                        {
                            isComputedColumn = Equals(row["Expression"], String.Empty);
                        }
                        else
                        {
                            isComputedColumn = !(row["Expression"] is DBNull);
                        }

                        if (isComputedColumn && !options.IncludeComputedColumns)
                            continue;
                    }

                    var col = new ProtoDataColumn
                    {
                        ColumnIndex = i,
                        ProtoDataType = ConvertProtoDataType.FromClrType((Type)row["DataType"]),
                        ColumnName = (string)row["ColumnName"]
                    };

                    columns.Add(col);
                }

                return columns;
            }
        }
    }
}