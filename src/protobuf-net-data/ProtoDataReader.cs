// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

#pragma warning disable 1591
using System;
using System.Collections.Generic;
using System.Data;
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
        private readonly List<ColReader> colReaders;

        private Stream stream;
        private object[] currentRow;
        private DataTable dataTable;
        private ProtoReader reader;
        private int currentField;
        private SubItemToken currentTableToken;
        private bool reachedEndOfCurrentTable;
        private bool isClosed;

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
            this.colReaders = new List<ColReader>();

            this.AdvanceToNextField();
            if (this.currentField != 1)
            {
                throw new InvalidOperationException("No results found! Invalid/corrupt stream.");
            }

            this.ReadNextTableHeader();
        }

        ~ProtoDataReader()
        {
            this.Dispose(false);
        }

        private delegate object ColReader();

        public int FieldCount
        {
            get
            {
                this.ThrowIfClosed();

                return this.dataTable.Columns.Count;
            }
        }

        public int Depth
        {
            get
            {
                this.ThrowIfClosed();

                return 1;
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

            return this.dataTable.Columns[i].ColumnName;
        }

        public string GetDataTypeName(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfIndexOutOfRange(i);

            return this.dataTable.Columns[i].DataType.Name;
        }

        public Type GetFieldType(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfIndexOutOfRange(i);

            return this.dataTable.Columns[i].DataType;
        }

        public object GetValue(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);

            return this.currentRow[i];
        }

        public int GetValues(object[] values)
        {
            Throw.IfNull(values, nameof(values));

            this.ThrowIfClosed();
            this.ThrowIfNoData();

            int length = Math.Min(values.Length, this.dataTable.Columns.Count);

            Array.Copy(this.currentRow, values, length);

            return length;
        }

        public int GetOrdinal(string name)
        {
            this.ThrowIfClosed();

            var column = this.dataTable.Columns[name];

            if (column == null)
            {
                throw new IndexOutOfRangeException(name);
            }

            return column.Ordinal;
        }

        public bool GetBoolean(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return (bool)this.currentRow[i];
        }

        public byte GetByte(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return (byte)this.currentRow[i];
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferOffset, int length)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return this.CopyArray((byte[])this.currentRow[i], fieldOffset, buffer, bufferOffset, length);
        }

        public char GetChar(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return (char)this.currentRow[i];
        }

        public long GetChars(int i, long fieldOffset, char[] buffer, int bufferOffset, int length)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return this.CopyArray((char[])this.currentRow[i], fieldOffset, buffer, bufferOffset, length);
        }

        public Guid GetGuid(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return (Guid)this.currentRow[i];
        }

        public short GetInt16(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return (short)this.currentRow[i];
        }

        public int GetInt32(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return (int)this.currentRow[i];
        }

        public long GetInt64(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return (long)this.currentRow[i];
        }

        public float GetFloat(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return (float)this.currentRow[i];
        }

        public double GetDouble(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return (double)this.currentRow[i];
        }

        public string GetString(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return (string)this.currentRow[i];
        }

        public decimal GetDecimal(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return (decimal)this.currentRow[i];
        }

        public DateTime GetDateTime(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return (DateTime)this.currentRow[i];
        }

        public IDataReader GetData(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return ((DataTable)this.currentRow[i]).CreateDataReader();
        }

        public bool IsDBNull(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);

            return this.currentRow[i] == null || this.currentRow[i] is DBNull;
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

            if (this.dataTable != null)
            {
                this.dataTable.Dispose();
                this.dataTable = null;
            }

            this.isClosed = true;
        }

        public bool NextResult()
        {
            this.ThrowIfClosed();

            this.ConsumeAnyRemainingRows();

            this.AdvanceToNextField();

            if (this.currentField == 0)
            {
                return false;
            }

            this.reachedEndOfCurrentTable = false;

            this.ReadNextTableHeader();

            return true;
        }

        public DataTable GetSchemaTable()
        {
            this.ThrowIfClosed();

            using (var schemaReader = this.dataTable.CreateDataReader())
            {
                return schemaReader.GetSchemaTable();
            }
        }

        public bool Read()
        {
            this.ThrowIfClosed();

            if (this.reachedEndOfCurrentTable)
            {
                return false;
            }

            if (this.currentField == 0)
            {
                ProtoReader.EndSubItem(this.currentTableToken, this.reader);
                this.reachedEndOfCurrentTable = true;
                return false;
            }

            this.ReadCurrentRow();
            this.AdvanceToNextField();

            return true;
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

        private void ConsumeAnyRemainingRows()
        {
            // Unfortunately, protocol buffers doesn't let you seek - we have
            // to consume all the remaining tokens up anyway
            while (this.Read())
            {
            }
        }

        private void ReadNextTableHeader()
        {
            this.ResetSchemaTable();
            this.currentRow = null;

            this.currentTableToken = ProtoReader.StartSubItem(this.reader);

            this.AdvanceToNextField();

            if (this.currentField == 0)
            {
                this.reachedEndOfCurrentTable = true;
                ProtoReader.EndSubItem(this.currentTableToken, this.reader);
                return;
            }

            if (this.currentField != 2)
            {
                throw new InvalidOperationException("No header found! Invalid/corrupt stream.");
            }

            this.ReadHeader();
        }

        private void AdvanceToNextField()
        {
            this.currentField = this.reader.ReadFieldHeader();
        }

        private void ResetSchemaTable()
        {
            this.dataTable = new DataTable();
            this.colReaders.Clear();
        }

        private void ReadHeader()
        {
            do
            {
                this.ReadColumn();
                this.AdvanceToNextField();
            }
            while (this.currentField == 2);
        }

        private void ReadColumn()
        {
            var token = ProtoReader.StartSubItem(this.reader);
            int field;
            string name = null;
            var protoDataType = (ProtoDataType)(-1);
            while ((field = this.reader.ReadFieldHeader()) != 0)
            {
                switch (field)
                {
                    case 1:
                        name = this.reader.ReadString();
                        break;
                    case 2:
                        protoDataType = (ProtoDataType)this.reader.ReadInt32();
                        break;
                    default:
                        this.reader.SkipField();
                        break;
                }
            }

            switch (protoDataType)
            {
                case ProtoDataType.Int:
                    this.colReaders.Add(() => this.reader.ReadInt32());
                    break;
                case ProtoDataType.Short:
                    this.colReaders.Add(() => this.reader.ReadInt16());
                    break;
                case ProtoDataType.Decimal:
                    this.colReaders.Add(() => BclHelpers.ReadDecimal(this.reader));
                    break;
                case ProtoDataType.String:
                    this.colReaders.Add(() => this.reader.ReadString());
                    break;
                case ProtoDataType.Guid:
                    this.colReaders.Add(() => BclHelpers.ReadGuid(this.reader));
                    break;
                case ProtoDataType.DateTime:
                    this.colReaders.Add(() => BclHelpers.ReadDateTime(this.reader));
                    break;
                case ProtoDataType.Bool:
                    this.colReaders.Add(() => this.reader.ReadBoolean());
                    break;

                case ProtoDataType.Byte:
                    this.colReaders.Add(() => this.reader.ReadByte());
                    break;

                case ProtoDataType.Char:
                    this.colReaders.Add(() => (char)this.reader.ReadInt16());
                    break;

                case ProtoDataType.Double:
                    this.colReaders.Add(() => this.reader.ReadDouble());
                    break;

                case ProtoDataType.Float:
                    this.colReaders.Add(() => this.reader.ReadSingle());
                    break;

                case ProtoDataType.Long:
                    this.colReaders.Add(() => this.reader.ReadInt64());
                    break;

                case ProtoDataType.ByteArray:
                    this.colReaders.Add(() => ProtoReader.AppendBytes(null, this.reader));
                    break;

                case ProtoDataType.CharArray:
                    this.colReaders.Add(() => this.reader.ReadString().ToCharArray());
                    break;

                case ProtoDataType.TimeSpan:
                    this.colReaders.Add(() => BclHelpers.ReadTimeSpan(this.reader));
                    break;

                default:
                    throw new NotSupportedException(protoDataType.ToString());
            }

            ProtoReader.EndSubItem(token, this.reader);
            this.dataTable.Columns.Add(name, ConvertProtoDataType.ToClrType(protoDataType));
        }

        private void ReadCurrentRow()
        {
            int field;

            if (this.currentRow == null)
            {
                this.currentRow = new object[this.colReaders.Count];
            }
            else
            {
                Array.Clear(this.currentRow, 0, this.currentRow.Length);
            }

            SubItemToken token = ProtoReader.StartSubItem(this.reader);
            while ((field = this.reader.ReadFieldHeader()) != 0)
            {
                if (field > this.currentRow.Length)
                {
                    this.reader.SkipField();
                }
                else
                {
                    int i = field - 1;
                    this.currentRow[i] = this.colReaders[i]();
                }
            }

            ProtoReader.EndSubItem(token, this.reader);
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
            if (i < 0 || i >= this.dataTable.Columns.Count)
            {
                throw new IndexOutOfRangeException();
            }
        }

        private void ThrowIfNoData()
        {
            if (this.reachedEndOfCurrentTable || this.currentRow == null)
            {
                throw new InvalidOperationException("Invalid attempt to read when no data is present.");
            }
        }

        private void ThrowIfNoValueIsNull(int i)
        {
            if (this.currentRow[i] == null)
            {
                throw new InvalidOperationException("Invalid attempt to read when no data is present.");
            }
        }
    }
}

#pragma warning restore 1591