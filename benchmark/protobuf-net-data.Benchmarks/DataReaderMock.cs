// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Data;

namespace ProtoBuf.Data.Benchmarks
{
    public class DataReaderMock : IDataReader
    {
        private readonly DataTable[] schemaTables;

        private int resultIndex;
        private int rowIndex = -1;

        public DataReaderMock(params DataTable[] schemaTables)
        {
            this.schemaTables = schemaTables;
        }

        public int Depth => throw new NotImplementedException();

        public bool IsClosed => throw new NotImplementedException();

        public int RecordsAffected => throw new NotImplementedException();

        public int FieldCount => throw new NotImplementedException();

        public object this[int i] => this.GetDefault((Type)this.schemaTables[this.resultIndex].Rows[i][1]);

        public object this[string name] => throw new NotImplementedException();

        public void Close()
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public bool GetBoolean(int i)
        {
            return true;
        }

        public byte GetByte(int i)
        {
            return 0b0010_1010;
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            return 0;
        }

        public char GetChar(int i)
        {
            return 'a';
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            return 0;
        }

        public IDataReader GetData(int i)
        {
            throw new NotImplementedException();
        }

        public string GetDataTypeName(int i)
        {
            throw new NotImplementedException();
        }

        public DateTime GetDateTime(int i)
        {
            return new DateTime(1969, 10, 29, 22, 30, 0);
        }

        public decimal GetDecimal(int i)
        {
            return 42m;
        }

        public double GetDouble(int i)
        {
            return 42d;
        }

        public Type GetFieldType(int i)
        {
            throw new NotImplementedException();
        }

        public float GetFloat(int i)
        {
            return 42f;
        }

        public Guid GetGuid(int i)
        {
            return Guid.NewGuid();
        }

        public short GetInt16(int i)
        {
            return 42;
        }

        public int GetInt32(int i)
        {
            return 42;
        }

        public long GetInt64(int i)
        {
            return 42L;
        }

        public string GetName(int i)
        {
            return (string)this.schemaTables[this.resultIndex].Rows[i][0];
        }

        public int GetOrdinal(string name)
        {
            throw new NotImplementedException();
        }

        public DataTable GetSchemaTable()
        {
            return this.schemaTables[this.resultIndex];
        }

        public string GetString(int i)
        {
            return "foo";
        }

        public object GetValue(int i)
        {
            throw new NotImplementedException();
        }

        public int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public bool IsDBNull(int i)
        {
            return false;
        }

        public bool NextResult()
        {
            if (this.resultIndex < this.schemaTables.Length - 1)
            {
                this.resultIndex++;

                this.rowIndex = 0;

                return true;
            }

            return false;
        }

        public bool Read()
        {
            return ++this.rowIndex < 1;
        }

        private object GetDefault(Type type)
        {
            if (type.IsValueType)
            {
                return Activator.CreateInstance(type);
            }

            return null;
        }
    }
}
