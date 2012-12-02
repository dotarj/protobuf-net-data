using System;
using System.Collections.Generic;
using System.Data;

namespace ProtoBuf.Data.Internal
{
    internal class ProtoDataColumnFactory
    {
        public IEnumerable<ProtoDataColumn> GetColumns(IDataReader reader, ProtoDataWriterOptions options)
        {
            if (reader == null) throw new ArgumentNullException("reader");
            if (options == null) throw new ArgumentNullException("options");

            using (var schema = reader.GetSchemaTable())
            {
                var schemaSupportsExpressions = schema.Columns.Contains("Expression");

                var columns = new List<ProtoDataColumn>(schema.Rows.Count);
                for (var i = 0; i < schema.Rows.Count; i++)
                {
                    // Assumption: rows in the schema table are always ordered by
                    // Ordinal position, ascending
                    var row = schema.Rows[i];

                    // Skip computed columns unless requested.
                    var isComputedColumn = schemaSupportsExpressions && !(row["Expression"] is DBNull);
                    if (isComputedColumn && !options.IncludeComputedColumns)
                        continue;

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