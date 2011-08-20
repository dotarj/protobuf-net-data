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
        readonly object syncRoot = new object();

        public ProtoDataReader(Stream stream)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            this.stream = stream;
            reader = new ProtoReader(stream, null, null);
            colReaders = new List<Func<object>>();
            dataTable = new DataTable();

            AdvanceToNextField();
            if (currentField != 2)
                throw new InvalidOperationException("No header found! Invalid/corrupt stream.");

            ReadHeader();
        }

        void AdvanceToNextField()
        {
            currentField = reader.ReadFieldHeader();
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


        DataTable dataTable;

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
            throw new NotImplementedException();
        }

        public char GetChar(int i)
        {
            return (char)currentRow[i];
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
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
            // Nothing to close
        }

        static Exception NestingNotSupported()
        {
            return new NotImplementedException("Nested data sets are not currently supported.");
        }

        public DataTable GetSchemaTable()
        {
            using (var reader = dataTable.CreateDataReader())
                return reader.GetSchemaTable();
        }


        public bool NextResult()
        {
            return false;
        }

        public bool Read()
        {
            if (IsClosed)
                return false;

            ReadCurrentRow();
            AdvanceToNextField();

            if (currentField == 0)
                IsClosed = true;

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

        bool disposed;
        private List<Func<object>> colReaders;
        private ProtoReader reader;
        private int currentField;
    }
}