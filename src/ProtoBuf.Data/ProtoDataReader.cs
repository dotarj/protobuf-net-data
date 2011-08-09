using System;
using System.Data;
using System.IO;
using System.Linq;
using ProtoBuf.Data.Internal;

namespace ProtoBuf.Data
{
    public class ProtoDataReader : IDataReader
    {
        Stream stream;
        ProtoDataRow currentRow;
        ProtoDataHeader header;
        readonly object syncRoot = new object();

        public ProtoDataReader(Stream stream)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            this.stream = stream;
            InitializeHeader();
        }
        
        DataTable headerAsDataTable;

        public string GetName(int i)
        {
            return ColumnAtIndex(i).ColumnName;
        }

        ProtoDataColumn ColumnAtIndex(int index)
        {
            return header.Columns.Single(c => c.Ordinal == index);
        }

        public string GetDataTypeName(int i)
        {
            return ColumnAtIndex(i).ProtoDataType.ToString();
        }

        public Type GetFieldType(int i)
        {
            return ConvertProtoDataType.ToClrType(ColumnAtIndex(i).ProtoDataType);
        }

        public object GetValue(int i)
        {
            if (currentRow == null)
                return null;

            if (currentRow.NullColumns[i])
                return null;

            var column = ColumnAtIndex(i);
            switch (column.ProtoDataType)
            {
                case ProtoDataType.Bool:
                    return currentRow.BoolValues[column.OrdinalWithinType];

                case ProtoDataType.Byte:
                    return currentRow.ByteValues[column.OrdinalWithinType];
                case ProtoDataType.Char:
                    return currentRow.CharValues[column.OrdinalWithinType];
                case ProtoDataType.DateTime:
                    return currentRow.DateTimeValues[column.OrdinalWithinType];
                case ProtoDataType.Decimal:
                    return currentRow.DecimalValues[column.OrdinalWithinType];
                case ProtoDataType.Double:
                    return currentRow.DoubleValues[column.OrdinalWithinType];
                case ProtoDataType.Float:
                    return currentRow.FloatValues[column.OrdinalWithinType];
                case ProtoDataType.Guid:
                    return currentRow.GuidValues[column.OrdinalWithinType];
                case ProtoDataType.Int:
                    return currentRow.Int32Values[column.OrdinalWithinType];
                case ProtoDataType.Long:
                    return currentRow.Int64Values[column.OrdinalWithinType];
                case ProtoDataType.Short:
                    return currentRow.Int16Values[column.OrdinalWithinType];
                case ProtoDataType.String:
                    return currentRow.StringValues[column.OrdinalWithinType];
            }

            throw new InvalidOperationException("Unknown data type.");
        }

        public int GetValues(object[] values)
        {
            var count = Math.Min(values.Length, header.Columns.Count);

            foreach (var column in header.Columns)
                values[column.Ordinal] = GetValue(column.Ordinal);

            return count;
        }

        public int GetOrdinal(string name)
        {
            if (header.Columns == null)
                throw new InvalidOperationException();

            return header.Columns.Single(c => c.ColumnName.Equals(name)).Ordinal;
        }

        int GetOrdinalWithinType(int i)
        {
            return header.Columns.Single(c => c.Ordinal == i).OrdinalWithinType;
        }

        public bool GetBoolean(int i)
        {
            return currentRow.BoolValues[GetOrdinalWithinType(i)];
        }

        public byte GetByte(int i)
        {
            return currentRow.ByteValues[GetOrdinalWithinType(i)];
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public char GetChar(int i)
        {
            return currentRow.CharValues[GetOrdinalWithinType(i)];
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public Guid GetGuid(int i)
        {
            return currentRow.GuidValues[GetOrdinalWithinType(i)];
        }

        public short GetInt16(int i)
        {
            return currentRow.Int16Values[GetOrdinalWithinType(i)];
        }

        public int GetInt32(int i)
        {
            return currentRow.Int32Values[GetOrdinalWithinType(i)];
        }

        public long GetInt64(int i)
        {
            return currentRow.Int64Values[GetOrdinalWithinType(i)];
        }

        public float GetFloat(int i)
        {
            return currentRow.FloatValues[GetOrdinalWithinType(i)];
        }

        public double GetDouble(int i)
        {
            return currentRow.DoubleValues[GetOrdinalWithinType(i)];
        }

        public string GetString(int i)
        {
            return currentRow.StringValues[i];
        }

        public decimal GetDecimal(int i)
        {
            return currentRow.DecimalValues[GetOrdinalWithinType(i)];
        }

        public DateTime GetDateTime(int i)
        {
            return currentRow.DateTimeValues[GetOrdinalWithinType(i)];
        }

        public IDataReader GetData(int i)
        {
            throw NestingNotSupported();
        }

        public bool IsDBNull(int i)
        {
            return currentRow.NullColumns[i];
        }

        public int FieldCount
        {
            get { return header.Columns.Count; }
        }

        object IDataRecord.this[int i]
        {
            get { throw NestingNotSupported(); }
        }

        object IDataRecord.this[string name]
        {
            get { throw NestingNotSupported(); }
        }

        public void Close()
        {
            // Nothing to close
        }

        static Exception NestingNotSupported()
        {
            return new NotImplementedException("Nested data sets are not currently supported.");
        }

        public DataTable GetSchemaTable()
        {
            InitializeHeader();

            using (var reader = headerAsDataTable.CreateDataReader())
                return reader.GetSchemaTable();
        }

        void InitializeHeader()
        {
            if (header == null)
            {
                lock (syncRoot)
                {
                    if (header != null)
                        return;

                    // How to make a DataTable correctly populated with all
                    // the columns required by a schema table? The easiest way
                    // is to create a dummy DataTable with the same columns as
                    // the protocol buffers stream, and use it's schema table.
                    header = Serializer.DeserializeWithLengthPrefix<ProtoDataHeader>(stream, PrefixStyle.Fixed32);
                    if (header == null)
                        throw new InvalidOperationException("Corrupt stream; could not parse ProtoDataHeader from Protocol Buffers stream.");

                    headerAsDataTable = new DataTable();
                    foreach (var column in header.Columns)
                        headerAsDataTable.Columns.Add(column.ColumnName, ConvertProtoDataType.ToClrType(column.ProtoDataType));
                }
            }
        }

        public bool NextResult()
        {
            return false;
        }

        public bool Read()
        {
            InitializeHeader();
            currentRow = Serializer.DeserializeWithLengthPrefix<ProtoDataRow>(stream, PrefixStyle.Fixed32);
            if (currentRow == null)
                IsClosed = true;

            return !IsClosed;
        }

        public int Depth
        {
            get { return 1; }
        }

        public bool IsClosed { get; private set; }

        public int RecordsAffected
        {
            get { return 0; }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposed)
            {
                if (disposing)
                {
                    if (stream != null)
                    {
                        stream.Dispose();
                        stream = null;
                    }

                    if (headerAsDataTable != null)
                    {
                        headerAsDataTable.Dispose();
                        headerAsDataTable = null;
                    }
                }

                disposed = true;
            }
        }

        ~ProtoDataReader()
        {
            Dispose(false);
        }

        bool disposed;
    }
}