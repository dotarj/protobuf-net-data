// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

#pragma warning disable 1591
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using ProtoBuf.Data.Internal;
#if NET45 || NETSTANDARD20
    using System.Runtime.CompilerServices;
#endif

namespace ProtoBuf.Data
{
    /// <summary>
    /// A custom <see cref="System.Data.IDataReader"/> for de-serializing a protocol-buffer binary stream back
    /// into a tabular form.
    /// </summary>
    public sealed class ProtoDataReader : IDataReader
    {
        private readonly ProtoReaderContext context;
        private Stream stream;
        private ProtoReader reader;
        private bool isClosed;
        private DataTable schemaTable;

        /// <summary>
        /// Initializes a new instance of the <see cref="ProtoDataReader"/> class.
        /// </summary>
        /// <param name="stream">
        /// The <see cref="System.IO.Stream"/> to read from.
        /// </param>
        public ProtoDataReader(Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("stream");
            }

            this.stream = stream;
            this.reader = new ProtoReader(stream, null, null);
            this.context = new ProtoReaderContext(this.reader);

            ResultReader.ReadResult(this.context);
        }

        ~ProtoDataReader()
        {
            this.Dispose(false);
        }

        public int FieldCount
        {
            get
            {
                this.ThrowIfClosed();

                return this.context.Columns.Count;
            }
        }

        public int Depth
        {
            get
            {
                this.ThrowIfClosed();

                return 0;
            }
        }

        public bool IsClosed => this.isClosed;

        /// <summary>
        /// Gets the number of rows changed, inserted, or deleted.
        /// </summary>
        /// <returns>This is always zero in the case of <see cref="ProtoBuf.Data.ProtoDataReader" />.</returns>
        public int RecordsAffected
        {
            get { return 0; }
        }

        object IDataRecord.this[int i]
        {
            get
            {
                return this.GetValue(i);
            }
        }

        object IDataRecord.this[string name]
        {
            get
            {
                return this.GetValue(this.GetOrdinal(name));
            }
        }

        public string GetName(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfIndexOutOfRange(i);

            return this.context.Columns[i].Name;
        }

        public string GetDataTypeName(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfIndexOutOfRange(i);

            return this.context.Columns[i].DataType.Name;
        }

        public Type GetFieldType(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfIndexOutOfRange(i);

            return this.context.Columns[i].DataType;
        }

        public object GetValue(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);

            return this.context.Buffers[i].Value;
        }

        public int GetValues(object[] values)
        {
            Throw.IfNull(values, nameof(values));

            this.ThrowIfClosed();
            this.ThrowIfNoData();

            var valuesCount = values.Length < this.context.Columns.Count ? values.Length : this.context.Columns.Count;

            for (var i = 0; i < valuesCount; i++)
            {
                values[i] = this.context.Buffers[i].Value;
            }

            return valuesCount;
        }

        public int GetOrdinal(string name)
        {
            this.ThrowIfClosed();

            var ordinal = this.GetColumnOrdinalByName(name);

            if (!ordinal.HasValue)
            {
                throw new IndexOutOfRangeException(name);
            }

            return ordinal.Value;
        }

        public bool GetBoolean(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return this.context.Buffers[i].Bool;
        }

        public byte GetByte(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return this.context.Buffers[i].Byte;
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferOffset, int length)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return this.CopyArray(this.context.Buffers[i].ByteArray, fieldOffset, buffer, bufferOffset, length);
        }

        public char GetChar(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return this.context.Buffers[i].Char;
        }

        public long GetChars(int i, long fieldOffset, char[] buffer, int bufferOffset, int length)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return this.CopyArray(this.context.Buffers[i].CharArray, fieldOffset, buffer, bufferOffset, length);
        }

        public Guid GetGuid(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return this.context.Buffers[i].Guid;
        }

        public short GetInt16(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return this.context.Buffers[i].Short;
        }

        public int GetInt32(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return this.context.Buffers[i].Int;
        }

        public long GetInt64(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return this.context.Buffers[i].Long;
        }

        public float GetFloat(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return this.context.Buffers[i].Float;
        }

        public double GetDouble(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return this.context.Buffers[i].Double;
        }

        public string GetString(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return this.context.Buffers[i].String;
        }

        public decimal GetDecimal(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return this.context.Buffers[i].Decimal;
        }

        public DateTime GetDateTime(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return this.context.Buffers[i].DateTime;
        }

        IDataReader IDataRecord.GetData(int i)
        {
            throw new NotSupportedException();
        }

        public bool IsDBNull(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);

            return this.context.Buffers[i].IsNull;
        }

        public void Close()
        {
            if (this.reader != null)
            {
                this.reader.Dispose();
                this.reader = null;
            }

            if (this.stream != null)
            {
                this.stream.Dispose();
                this.stream = null;
            }

            this.isClosed = true;
        }

        public bool NextResult()
        {
            this.ThrowIfClosed();

            this.ConsumeAnyRemainingRows();

            this.schemaTable = null;
            this.context.Columns = new List<ProtoDataColumn>();

            return ResultReader.ReadResult(this.context);
        }

        public DataTable GetSchemaTable()
        {
            this.ThrowIfClosed();

            if (this.schemaTable == null)
            {
                this.schemaTable = this.BuildSchemaTable();
            }

            return this.schemaTable;
        }

        public bool Read()
        {
            this.ThrowIfClosed();

            return RecordReader.ReadRecord(this.context);
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (disposing)
            {
                this.Close();
            }
        }

        private int? GetColumnOrdinalByName(string name)
        {
            for (var ordinal = 0; ordinal < this.context.Columns.Count; ordinal++)
            {
                if (name == this.context.Columns[ordinal].Name)
                {
                    return ordinal;
                }
            }

            return null;
        }

        private DataTable BuildSchemaTable()
        {
            var schemaTable = new DataTable("SchemaTable")
            {
                Locale = CultureInfo.InvariantCulture,
                MinimumCapacity = this.context.Columns.Count
            };

            var columnName = new DataColumn("ColumnName", typeof(string));
            var columnOrdinal = new DataColumn("ColumnOrdinal", typeof(int)) { DefaultValue = 0 };
            var columnSize = new DataColumn("ColumnSize", typeof(int)) { DefaultValue = -1 };
            var dataType = new DataColumn("DataType", typeof(Type));
            var dataTypeName = new DataColumn("DataTypeName", typeof(string));

            schemaTable.Columns.Add(columnName);
            schemaTable.Columns.Add(columnOrdinal);
            schemaTable.Columns.Add(columnSize);
            schemaTable.Columns.Add(dataType);
            schemaTable.Columns.Add(dataTypeName);

            for (var ordinal = 0; ordinal < this.context.Columns.Count; ordinal++)
            {
                var schemaRow = schemaTable.NewRow();

                schemaRow[columnName] = this.context.Columns[ordinal].Name;
                schemaRow[columnOrdinal] = ordinal;
                schemaRow[dataType] = this.context.Columns[ordinal].DataType;
                schemaRow[dataTypeName] = this.context.Columns[ordinal].DataType.Name;

                schemaTable.Rows.Add(schemaRow);
            }

            foreach (DataColumn column in schemaTable.Columns)
            {
                column.ReadOnly = true;
            }

            return schemaTable;
        }

        private void ConsumeAnyRemainingRows()
        {
            // Unfortunately, protocol buffers doesn't let you seek - we have
            // to consume all the remaining tokens up anyway
            while (this.Read())
            {
            }
        }

        private long CopyArray(Array source, long fieldOffset, Array buffer, int bufferOffset, int length)
        {
            // Partial implementation of SqlDataReader.GetBytes.
            if (fieldOffset < 0)
            {
                throw new InvalidOperationException("Invalid value for argument 'fieldOffset'. The value must be greater than or equal to 0.");
            }

            if (length < 0)
            {
                throw new IndexOutOfRangeException($"Data length '{length}' is less than 0.");
            }

            var copyLength = source.LongLength;

            if (buffer == null)
            {
                return copyLength;
            }

            if (bufferOffset < 0 || bufferOffset >= buffer.Length)
            {
                throw new ArgumentOutOfRangeException("bufferOffset", $"Invalid destination buffer (size of {buffer.Length}) offset: {bufferOffset}.");
            }

            if (copyLength + bufferOffset > buffer.Length)
            {
                throw new IndexOutOfRangeException($"Buffer offset '{bufferOffset}' plus the elements available '{copyLength}' is greater than the length of the passed in buffer.");
            }

            if (fieldOffset >= copyLength)
            {
                return 0;
            }

            if (fieldOffset + length > copyLength)
            {
                copyLength = copyLength - fieldOffset;
            }
            else
            {
                copyLength = length;
            }

            Array.Copy(source, fieldOffset, buffer, bufferOffset, copyLength);

            return copyLength;
        }

#if NET45 || NETSTANDARD20
        private void ThrowIfClosed([CallerMemberName]string memberName = "")
        {
            if (this.IsClosed)
            {
                throw new InvalidOperationException($"Invalid attempt to call {memberName} when reader is closed.");
            }
        }
#else
        private void ThrowIfClosed()
        {
            if (this.IsClosed)
            {
                throw new InvalidOperationException("Invalid attempt to call method when reader is closed.");
            }
        }
#endif

        private void ThrowIfIndexOutOfRange(int i)
        {
            if (i < 0 || i >= this.context.Columns.Count)
            {
                throw new IndexOutOfRangeException();
            }
        }

        private void ThrowIfNoData()
        {
            if (this.context.ReachedEndOfCurrentResult || this.context.Buffers == null)
            {
                throw new InvalidOperationException("Invalid attempt to read when no data is present.");
            }
        }

        private void ThrowIfNoValueIsNull(int i)
        {
            if (this.context.Buffers[i] == null)
            {
                throw new InvalidOperationException("Invalid attempt to read when no data is present.");
            }
        }
    }
}

#pragma warning restore 1591