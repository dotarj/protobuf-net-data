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
using System.IO;
using ProtoBuf.Data.Internal;

namespace ProtoBuf.Data
{
    ///<summary>
    /// A custom <see cref="System.Data.IDataReader"/> for deserializing a protocol-buffer binary stream back
    /// into a tabular form.
    ///</summary>
    public class ProtoDataReader : IDataReader
    {
        delegate object ColReader(); 
        Stream stream;
        object[] currentRow;
        DataTable dataTable;
        bool disposed;
        private readonly List<ColReader> colReaders;
        private ProtoReader reader;
        private int currentField;
        private SubItemToken currentTableToken;
        private bool reachedEndOfCurrentTable;

        /// <summary>
        /// Initializes a new instance of a <see cref="ProtoBuf.Data.ProtoDataReader"/> type.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to read from.</param>

        public ProtoDataReader(Stream stream)

        {
            if (stream == null) throw new ArgumentNullException("stream");
            this.stream = stream;
            reader = new ProtoReader(stream, null, null);
            colReaders = new List<ColReader>();

            AdvanceToNextField();
            if (currentField != 1)
                throw new InvalidOperationException("No results found! Invalid/corrupt stream.");

            ReadNextTableHeader();
        }

#pragma warning disable 1591 // Missing XML comment

        public string GetName(int i)
        {
            ErrorIfClosed();
            return dataTable.Columns[i].ColumnName;
        }

        public string GetDataTypeName(int i)
        {
            ErrorIfClosed();
            return dataTable.Columns[i].DataType.Name;
        }

        public Type GetFieldType(int i)
        {
            ErrorIfClosed();
            return dataTable.Columns[i].DataType;
        }

        public object GetValue(int i)
        {
            ErrorIfClosed();
            return currentRow[i];
        }

        public int GetValues(object[] values)
        {
            ErrorIfClosed();

            var length = Math.Min(values.Length, dataTable.Columns.Count);

            Array.Copy(currentRow, values, length);

            return length;
        }

        public int GetOrdinal(string name)
        {
            ErrorIfClosed();
            return dataTable.Columns[name].Ordinal;
        }

        public bool GetBoolean(int i)
        {
            ErrorIfClosed();
            return (bool)currentRow[i];
        }

        public byte GetByte(int i)
        {
            ErrorIfClosed();
            return (byte)currentRow[i];
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            ErrorIfClosed();
            var sourceBuffer = (byte[])currentRow[i];
            length = Math.Min(length, currentRow.Length - (int)fieldOffset);
            Array.Copy(sourceBuffer, fieldOffset, buffer, bufferoffset, length);
            return length;
        }

        public char GetChar(int i)
        {
            ErrorIfClosed();
            return (char)currentRow[i];
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            ErrorIfClosed();
            var sourceBuffer = (char[])currentRow[i];
            length = Math.Min(length, currentRow.Length - (int)fieldoffset);
            Array.Copy(sourceBuffer, fieldoffset, buffer, bufferoffset, length);
            return length;
        }

        public Guid GetGuid(int i)
        {
            ErrorIfClosed();
            return (Guid)currentRow[i];
        }

        public short GetInt16(int i)
        {
            ErrorIfClosed();
            return (short)currentRow[i];
        }

        public int GetInt32(int i)
        {
            ErrorIfClosed();
            return (int)currentRow[i];
        }

        public long GetInt64(int i)
        {
            ErrorIfClosed();
            return (long)currentRow[i];
        }

        public float GetFloat(int i)
        {
            ErrorIfClosed();
            return (float)currentRow[i];
        }

        public double GetDouble(int i)
        {
            ErrorIfClosed();
            return (double)currentRow[i];
        }

        public string GetString(int i)
        {
            ErrorIfClosed();
            return (string)currentRow[i];
        }

        public decimal GetDecimal(int i)
        {
            ErrorIfClosed();
            return (decimal)currentRow[i];
        }

        public DateTime GetDateTime(int i)
        {
            ErrorIfClosed();
            return (DateTime)currentRow[i];
        }

        public IDataReader GetData(int i)
        {
            ErrorIfClosed();
            return ((DataTable) currentRow[i]).CreateDataReader();
        }

        public bool IsDBNull(int i)
        {
            ErrorIfClosed();
            return currentRow[i] == null || currentRow[i] is DBNull;
        }

        public int FieldCount
        {
            get
            {
                ErrorIfClosed();
                return dataTable.Columns.Count;
            }
        }

        object IDataRecord.this[int i]
        {
            get
            {
                ErrorIfClosed();
                return GetValue(i);
            }
        }

        object IDataRecord.this[string name]
        {
            get
            {
                ErrorIfClosed();
                return GetValue(GetOrdinal(name));
            }
        }

        public void Close()
        {
            stream.Close();
            IsClosed = true;
        }

        public bool NextResult()
        {
            ErrorIfClosed();

            ConsumeAnyRemainingRows();

            AdvanceToNextField();

            if (currentField == 0)
            {
                IsClosed = true;
                return false;
            }

            reachedEndOfCurrentTable = false;

            ReadNextTableHeader();

            return true;
        }

        public DataTable GetSchemaTable()
        {
            ErrorIfClosed();
            using (var schemaReader = dataTable.CreateDataReader())
                return schemaReader.GetSchemaTable();
        }

        public bool Read()
        {
            ErrorIfClosed();

            if (reachedEndOfCurrentTable)
                return false;

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

        public int Depth
        {
            get
            {
                ErrorIfClosed();
                return 1;
            }
        }

        public bool IsClosed { get; private set; }

        /// <summary>
        /// Gets the number of rows changed, inserted, or deleted. 
        /// </summary>
        /// <returns>This is always zero in the case of <see cref="ProtoBuf.Data.ProtoDataReader" />.</returns>
        public int RecordsAffected
        {
            get { return 0; }
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

        ~ProtoDataReader()
        {
            Dispose(false);
        }

#pragma warning restore 1591 // Missing XML comment

        private void ConsumeAnyRemainingRows()
        {
            // Unfortunately, protocol buffers doesn't let you seek - we have
            // to consume all the remaining tokens up anyway
            while (Read());
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
                throw new InvalidOperationException("No header found! Invalid/corrupt stream.");

            ReadHeader();
        }

        void AdvanceToNextField()
        {
            currentField = reader.ReadFieldHeader();
        }

        void ResetSchemaTable()
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
            } while (currentField == 2);
        }

        void ReadColumn()
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
                currentRow = new object[colReaders.Count];
            else
                Array.Clear(currentRow, 0, currentRow.Length);

            var token = ProtoReader.StartSubItem(reader);
            while ((field = reader.ReadFieldHeader()) != 0)
            {
                if (field > currentRow.Length) reader.SkipField();
                else
                {
                    int i = field - 1;
                    currentRow[i] = colReaders[i]();
                }
            }
            ProtoReader.EndSubItem(token, reader);
        }

        private void ErrorIfClosed()
        {
            if (IsClosed)
                throw new InvalidOperationException("Attempt to access ProtoDataReader which was already closed.");
        }
    }
}