// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;

namespace ProtoBuf.Data.Internal
{
    internal sealed class ProtoDataColumnFactory
    {
        private static readonly bool IsRunningOnMono;

        static ProtoDataColumnFactory()
        {
            // From http://stackoverflow.com/a/721194/91551
            IsRunningOnMono = Type.GetType("Mono.Runtime") != null;
        }

        public IList<ProtoDataColumn> GetColumns(IDataReader reader, ProtoDataWriterOptions options)
        {
            if (reader == null)
            {
                throw new ArgumentNullException("reader");
            }

            if (options == null)
            {
                throw new ArgumentNullException("options");
            }

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

                        if (IsRunningOnMono)
                        {
                            isComputedColumn = Equals(row["Expression"], string.Empty);
                        }
                        else
                        {
                            isComputedColumn = !(row["Expression"] is DBNull);
                        }

                        if (isComputedColumn && !options.IncludeComputedColumns)
                        {
                            continue;
                        }
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