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
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using ProtoBuf.Data.Internal;

namespace ProtoBuf.Data
{
    public class ProtoDataReader : IDataReader
    {
        Stream stream;
        object[] currentRow;
        DataTable dataTable;
        bool disposed;
        private readonly List<Func<object>> colReaders;
        private ProtoReader reader;
        private int currentField;
        private SubItemToken currentTableToken;

        public ProtoDataReader(Stream stream)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            this.stream = stream;
            reader = new ProtoReader(stream, null, null);
            colReaders = new List<Func<object>>();

            ResetCurrentSchema(); // just in case they call FieldCount before Read()
            AdvanceToNextField();
            if (currentField != 1)
                throw new InvalidOperationException("No results found! Invalid/corrupt stream.");

            ReadNextTableHeader();
        }

        private void ReadNextTableHeader()
        {
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

        void ResetCurrentSchema()
        {
            dataTable = new DataTable();
            colReaders.Clear();
        }

        private void ReadHeader()
        {
            ResetCurrentSchema();

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
            var protoDataType = (ProtoDataType) (-1);
            while ((field = reader.ReadFieldHeader()) != 0)
            {
                switch (field)
                {
                    case 1:
                        name = reader.ReadString();
                        break;
                    case 2:
                        protoDataType = (ProtoDataType) reader.ReadInt32();
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
                    colReaders.Add(() => reader.ReadString().ToArray());
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

        public string GetName(int i)
        {
            return dataTable.Columns[i].ColumnName;
        }

        public string GetDataTypeName(int i)
        {
            return dataTable.Columns[i].DataType.Name;
        }

        public Type GetFieldType(int i)
        {
            return dataTable.Columns[i].DataType;
        }

        public object GetValue(int i)
        {
            return currentRow[i];
        }

        public int GetValues(object[] values)
        {
            var length = Math.Min(values.Length, dataTable.Columns.Count);

            Array.Copy(currentRow, values, length);

            return length;
        }

        public int GetOrdinal(string name)
        {
            return dataTable.Columns[name].Ordinal;
        }

        public bool GetBoolean(int i)
        {
            return (bool)currentRow[i];
        }

        public byte GetByte(int i)
        {
            return (byte)currentRow[i];
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            var sourceBuffer = (byte[]) currentRow[i];
            length = Math.Min(length, currentRow.Length - (int)fieldOffset);
            Array.Copy(sourceBuffer, fieldOffset, buffer, bufferoffset, length);
            return length;
        }

        public char GetChar(int i)
        {
            return (char)currentRow[i];
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            var sourceBuffer = (char[])currentRow[i];
            length = Math.Min(length, currentRow.Length - (int)fieldoffset);
            Array.Copy(sourceBuffer, fieldoffset, buffer, bufferoffset, length);
            return length;
        }

        public Guid GetGuid(int i)
        {
            return (Guid)currentRow[i];
        }

        public short GetInt16(int i)
        {
            return (short)currentRow[i];
        }

        public int GetInt32(int i)
        {
            return (int)currentRow[i];
        }

        public long GetInt64(int i)
        {
            return (long)currentRow[i];
        }

        public float GetFloat(int i)
        {
            return (float)currentRow[i];
        }

        public double GetDouble(int i)
        {
            return (double)currentRow[i];
        }

        public string GetString(int i)
        {
            return (string)currentRow[i];
        }

        public decimal GetDecimal(int i)
        {
            return (decimal)currentRow[i];
        }

        public DateTime GetDateTime(int i)
        {
            return (DateTime) currentRow[i];
        }

        public IDataReader GetData(int i)
        {
            throw NestingNotSupported();
        }

        public bool IsDBNull(int i)
        {
            return currentRow[i] == null || currentRow[i] is DBNull;
        }

        public int FieldCount
        {
            get { return dataTable.Columns.Count; }
        }

        object IDataRecord.this[int i]
        {
            get { return GetValue(i); }
        }

        object IDataRecord.this[string name]
        {
            get { return GetValue(GetOrdinal(name)); }
        }

        public void Close()
        {
            stream.Close();
            IsClosed = true;
        }

        static Exception NestingNotSupported()
        {
            return new NotImplementedException("Nested data sets are not currently supported.");
        }

        public DataTable GetSchemaTable()
        {
            using (var schemaReader = dataTable.CreateDataReader())
                return schemaReader.GetSchemaTable();
        }


        public bool NextResult()
        {
            if (IsClosed)
                return false;

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

        private void ConsumeAnyRemainingRows()
        {
            // Unfortunately, protocol buffers doesn't let you seek - we have
            // to consume all the remaining tokens up anyway
            while (Read());
        }

        private bool reachedEndOfCurrentTable;

        public bool Read()
        {
            if (IsClosed || reachedEndOfCurrentTable)
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
    }
}