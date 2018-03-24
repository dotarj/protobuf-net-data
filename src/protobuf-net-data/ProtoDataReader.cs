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

#pragma warning disable 1591
namespace ProtoBuf.Data
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.IO;
    using ProtoBuf.Data.Internal;
    using System.Runtime.CompilerServices;

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
        private bool disposed;
        private ProtoReader reader;
        private int currentField;
        private SubItemToken currentTableToken;
        private bool reachedEndOfCurrentTable;

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
            reader = new ProtoReader(stream, null, null);
            colReaders = new List<ColReader>();

            AdvanceToNextField();
            if (currentField != 1)
            {
                throw new InvalidOperationException("No results found! Invalid/corrupt stream.");
            }

            ReadNextTableHeader();
        }

        ~ProtoDataReader()
        {
            Dispose(false);
        }

        private delegate object ColReader();

        public int FieldCount
        {
            get
            {
                ThrowIfClosed();

                return dataTable.Columns.Count;
            }
        }

        public int Depth
        {
            get
            {
                ThrowIfClosed();

                return 1;
            }
        }

        public bool IsClosed => this.disposed;

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
                return GetValue(i);
            }
        }

        object IDataRecord.this[string name]
        {
            get
            {
                return GetValue(GetOrdinal(name));
            }
        }

        public string GetName(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfIndexOutOfRange(i);

            return dataTable.Columns[i].ColumnName;
        }

        public string GetDataTypeName(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfIndexOutOfRange(i);

            return dataTable.Columns[i].DataType.Name;
        }

        public Type GetFieldType(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfIndexOutOfRange(i);

            return dataTable.Columns[i].DataType;
        }

        public object GetValue(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);

            return currentRow[i];
        }

        public int GetValues(object[] values)
        {
            Throw.IfNull(values, nameof(values));

            this.ThrowIfClosed();
            this.ThrowIfNoData();

            int length = Math.Min(values.Length, dataTable.Columns.Count);

            Array.Copy(currentRow, values, length);

            return length;
        }

        public int GetOrdinal(string name)
        {
            ThrowIfClosed();

            var column = dataTable.Columns[name];

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

            return (bool)currentRow[i];
        }

        public byte GetByte(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return (byte)currentRow[i];
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferOffset, int length)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return this.CopyArray((byte[])currentRow[i], fieldOffset, buffer, bufferOffset, length);
        }

        public char GetChar(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return (char)currentRow[i];
        }

        public long GetChars(int i, long fieldOffset, char[] buffer, int bufferOffset, int length)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return this.CopyArray((char[])currentRow[i], fieldOffset, buffer, bufferOffset, length);
        }

        public Guid GetGuid(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return (Guid)currentRow[i];
        }

        public short GetInt16(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return (short)currentRow[i];
        }

        public int GetInt32(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return (int)currentRow[i];
        }

        public long GetInt64(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return (long)currentRow[i];
        }

        public float GetFloat(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return (float)currentRow[i];
        }

        public double GetDouble(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return (double)currentRow[i];
        }

        public string GetString(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return (string)currentRow[i];
        }

        public decimal GetDecimal(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return (decimal)currentRow[i];
        }

        public DateTime GetDateTime(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return (DateTime)currentRow[i];
        }

        public IDataReader GetData(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);
            this.ThrowIfNoValueIsNull(i);

            return ((DataTable)currentRow[i]).CreateDataReader();
        }

        public bool IsDBNull(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);

            return currentRow[i] == null || currentRow[i] is DBNull;
        }

        public void Close()
        {
            this.Dispose();
        }

        public bool NextResult()
        {
            ThrowIfClosed();

            ConsumeAnyRemainingRows();

            AdvanceToNextField();

            if (currentField == 0)
            {
                return false;
            }

            reachedEndOfCurrentTable = false;

            ReadNextTableHeader();

            return true;
        }

        public DataTable GetSchemaTable()
        {
            ThrowIfClosed();

            using (var schemaReader = dataTable.CreateDataReader())
            {
                return schemaReader.GetSchemaTable();
            }
        }

        public bool Read()
        {
            ThrowIfClosed();

            if (reachedEndOfCurrentTable)
            {
                return false;
            }

            if (currentField == 0)
            {
                ProtoReader.EndSubItem(currentTableToken, reader);
                reachedEndOfCurrentTable = true;
                return false;
            }

            ReadCurrentRow();
            AdvanceToNextField();

            return true;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (!disposed)
            {
                if (disposing)
                {
                    if (reader != null)
                    {
                        reader.Dispose();
                        reader = null;
                    }

                    if (stream != null)
                    {
                        stream.Dispose();
                        stream = null;
                    }

                    if (dataTable != null)
                    {
                        dataTable.Dispose();
                        dataTable = null;
                    }
                }

                disposed = true;
            }
        }

        private void ConsumeAnyRemainingRows()
        {
            // Unfortunately, protocol buffers doesn't let you seek - we have
            // to consume all the remaining tokens up anyway
            while (Read())
            {
            }
        }

        private void ReadNextTableHeader()
        {
            ResetSchemaTable();
            currentRow = null;

            currentTableToken = ProtoReader.StartSubItem(reader);

            AdvanceToNextField();

            if (currentField == 0)
            {
                reachedEndOfCurrentTable = true;
                ProtoReader.EndSubItem(currentTableToken, reader);
                return;
            }

            if (currentField != 2)
            {
                throw new InvalidOperationException("No header found! Invalid/corrupt stream.");
            }

            ReadHeader();
        }

        private void AdvanceToNextField()
        {
            currentField = reader.ReadFieldHeader();
        }

        private void ResetSchemaTable()
        {
            dataTable = new DataTable();
            colReaders.Clear();
        }

        private void ReadHeader()
        {
            do
            {
                ReadColumn();
                AdvanceToNextField();
            }
            while (currentField == 2);
        }

        private void ReadColumn()
        {
            var token = ProtoReader.StartSubItem(reader);
            int field;
            string name = null;
            var protoDataType = (ProtoDataType)(-1);
            while ((field = reader.ReadFieldHeader()) != 0)
            {
                switch (field)
                {
                    case 1:
                        name = reader.ReadString();
                        break;
                    case 2:
                        protoDataType = (ProtoDataType)reader.ReadInt32();
                        break;
                    default:
                        reader.SkipField();
                        break;
                }
            }

            switch (protoDataType)
            {
                case ProtoDataType.Int:
                    colReaders.Add(() => reader.ReadInt32());
                    break;
                case ProtoDataType.Short:
                    colReaders.Add(() => reader.ReadInt16());
                    break;
                case ProtoDataType.Decimal:
                    colReaders.Add(() => BclHelpers.ReadDecimal(reader));
                    break;
                case ProtoDataType.String:
                    colReaders.Add(() => reader.ReadString());
                    break;
                case ProtoDataType.Guid:
                    colReaders.Add(() => BclHelpers.ReadGuid(reader));
                    break;
                case ProtoDataType.DateTime:
                    colReaders.Add(() => BclHelpers.ReadDateTime(reader));
                    break;
                case ProtoDataType.Bool:
                    colReaders.Add(() => reader.ReadBoolean());
                    break;

                case ProtoDataType.Byte:
                    colReaders.Add(() => reader.ReadByte());
                    break;

                case ProtoDataType.Char:
                    colReaders.Add(() => (char)reader.ReadInt16());
                    break;

                case ProtoDataType.Double:
                    colReaders.Add(() => reader.ReadDouble());
                    break;

                case ProtoDataType.Float:
                    colReaders.Add(() => reader.ReadSingle());
                    break;

                case ProtoDataType.Long:
                    colReaders.Add(() => reader.ReadInt64());
                    break;

                case ProtoDataType.ByteArray:
                    colReaders.Add(() => ProtoReader.AppendBytes(null, reader));
                    break;

                case ProtoDataType.CharArray:
                    colReaders.Add(() => reader.ReadString().ToCharArray());
                    break;

                case ProtoDataType.TimeSpan:
                    colReaders.Add(() => BclHelpers.ReadTimeSpan(reader));
                    break;

                default:
                    throw new NotSupportedException(protoDataType.ToString());
            }

            ProtoReader.EndSubItem(token, reader);
            dataTable.Columns.Add(name, ConvertProtoDataType.ToClrType(protoDataType));
        }

        private void ReadCurrentRow()
        {
            int field;

            if (currentRow == null)
            {
                currentRow = new object[colReaders.Count];
            }
            else
            {
                Array.Clear(currentRow, 0, currentRow.Length);
            }

            SubItemToken token = ProtoReader.StartSubItem(reader);
            while ((field = reader.ReadFieldHeader()) != 0)
            {
                if (field > currentRow.Length)
                {
                    reader.SkipField();
                }
                else
                {
                    int i = field - 1;
                    currentRow[i] = colReaders[i]();
                }
            }

            ProtoReader.EndSubItem(token, reader);
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
            if (reachedEndOfCurrentTable || this.currentRow == null)
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