// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

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
    /// Provides a way of reading a forward-only stream of data rows from a data source. This class cannot be inherited.
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
        /// <param name="stream">A <see cref="Stream"/> that represents the stream the <see cref="ProtoDataReader"/> reads from.</param>
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

        /// <summary>
        /// Finalizes an instance of the <see cref="ProtoDataReader"/> class.
        /// </summary>
        ~ProtoDataReader()
        {
            this.Dispose(false);
        }

        /// <summary>
        /// Gets the number of columns in the current row.
        /// </summary>
        /// <returns>When not positioned in a valid recordset, 0; otherwise, the number of columns in the current record.</returns>
        public int FieldCount
        {
            get
            {
                this.ThrowIfClosed();

                return this.context.Columns.Count;
            }
        }

        /// <summary>
        /// Gets a value that indicates the depth of nesting for the current row.
        /// </summary>
        /// <returns>The depth of nesting for the current row.</returns>
        public int Depth
        {
            get
            {
                this.ThrowIfClosed();

                return 0;
            }
        }

        /// <summary>
        /// Gets a value indicating whether the specified <see cref="ProtoDataReader"/> instance has been closed.
        /// </summary>
        /// <returns>true if the data reader is closed; otherwise, false.</returns>
        public bool IsClosed => this.isClosed;

        /// <summary>
        /// Gets the number of rows changed, inserted, or deleted by execution of the SQL statement.
        /// </summary>
        /// <returns>The number of rows changed, inserted, or deleted; 0 if no rows were affected or the statement failed; and -1 for SELECT statements.</returns>
        /// <remarks>The <see cref="ProtoDataReader"/> does not support <see cref="IDataReader.RecordsAffected"/> and always returns zero.</remarks>
        public int RecordsAffected
        {
            get { return 0; }
        }

        /// <summary>
        /// Gets the value of the specified column in its native format given the column ordinal.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The value of the specified column in its native format.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        /// <exception cref="IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="IDataRecord.FieldCount"/>.</exception>
        public object this[int i]
        {
            get
            {
                return this.GetValue(i);
            }
        }

        /// <summary>
        /// Gets the value of the specified column in its native format given the column name.
        /// </summary>
        /// <param name="name">The column name.</param>
        /// <returns>The value of the specified column in its native format.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        /// <exception cref="IndexOutOfRangeException">No column with the specified name was found.</exception>
        public object this[string name]
        {
            get
            {
                return this.GetValue(this.GetOrdinal(name));
            }
        }

        /// <summary>
        /// Gets the name of the specified column.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The name of the specified column.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        /// <exception cref="IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="IDataRecord.FieldCount"/>.</exception>
        public string GetName(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfIndexOutOfRange(i);

            return this.context.Columns[i].Name;
        }

        /// <summary>
        /// Gets a string representing the data type of the specified column.
        /// </summary>
        /// <param name="i">The zero-based ordinal position of the column to find.</param>
        /// <returns>The string representing the data type of the specified column.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        /// <exception cref="IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="IDataRecord.FieldCount"/>.</exception>
        public string GetDataTypeName(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfIndexOutOfRange(i);

            return this.context.Columns[i].DataType.Name;
        }

        /// <summary>
        /// Gets the <see cref="Type"/> that is the data type of the object.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The <see cref="Type"/> that is the data type of the object. If the type does not exist on the client, in the case of a User-Defined Type (UDT) returned from the database, GetFieldType returns null.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        /// <exception cref="IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="IDataRecord.FieldCount"/>.</exception>
        public Type GetFieldType(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfIndexOutOfRange(i);

            return this.context.Columns[i].DataType;
        }

        /// <summary>
        /// Return the value of the specified field.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The <see cref="object"/> which will contain the field value upon return.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        /// <exception cref="IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="IDataRecord.FieldCount"/>.</exception>
        public object GetValue(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);

            return this.context.Buffers[i].Value;
        }

        /// <summary>
        /// Populates an array of objects with the column values of the current row.
        /// </summary>
        /// <param name="values">An array of <see cref="object"/> into which to copy the attribute columns.</param>
        /// <returns>The number of instances of <see cref="object"/> in the array.</returns>
        /// <exception cref="ArgumentNullException">values is null.</exception>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
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

        /// <summary>
        /// Gets the column ordinal, given the name of the column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>The zero-based column ordinal.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        /// <exception cref="IndexOutOfRangeException">No column with the specified name was found.</exception>
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

        /// <summary>
        /// Gets the value of the specified column as a Boolean.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The value of the column.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        /// <exception cref="IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="IDataRecord.FieldCount"/>.</exception>
        /// <exception cref="InvalidCastException">The specified cast is not valid.</exception>
        public bool GetBoolean(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);

            return this.context.Buffers[i].Boolean;
        }

        /// <summary>
        /// Gets the value of the specified column as a byte.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The value of the column.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        /// <exception cref="IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="IDataRecord.FieldCount"/>.</exception>
        /// <exception cref="InvalidCastException">The specified cast is not valid.</exception>
        public byte GetByte(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);

            return this.context.Buffers[i].Byte;
        }

        /// <summary>
        /// Reads a stream of bytes from the specified column offset into the buffer an array starting at the given buffer offset.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <param name="fieldOffset">The index within the field from which to begin the read operation.</param>
        /// <param name="buffer">The buffer into which to read the stream of bytes.</param>
        /// <param name="bufferOffset">The index within the buffer where the write operation is to start.</param>
        /// <param name="length">The maximum length to copy into the buffer.</param>
        /// <returns>The actual number of bytes read.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        /// <exception cref="IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="IDataRecord.FieldCount"/>.</exception>
        /// <exception cref="InvalidCastException">The specified cast is not valid.</exception>
        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferOffset, int length)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);

            return this.CopyArray(this.context.Buffers[i].ByteArray, fieldOffset, buffer, bufferOffset, length);
        }

        /// <summary>
        /// Gets the character value of the specified column.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The character value of the specified column.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        /// <exception cref="IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="IDataRecord.FieldCount"/>.</exception>
        /// <exception cref="InvalidCastException">The specified cast is not valid.</exception>
        public char GetChar(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);

            return this.context.Buffers[i].Char;
        }

        /// <summary>
        /// Reads a stream of characters from the specified column offset into the buffer as an array starting at the given buffer offset.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <param name="fieldOffset">The index within the field from which to begin the read operation.</param>
        /// <param name="buffer">The buffer into which to read the stream of bytes.</param>
        /// <param name="bufferOffset">The index within the buffer where the write operation is to start.</param>
        /// <param name="length">The maximum length to copy into the buffer.</param>
        /// <returns>The actual number of characters read.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        /// <exception cref="IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="IDataRecord.FieldCount"/>.</exception>
        /// <exception cref="InvalidCastException">The specified cast is not valid.</exception>
        public long GetChars(int i, long fieldOffset, char[] buffer, int bufferOffset, int length)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);

            return this.CopyArray(this.context.Buffers[i].CharArray, fieldOffset, buffer, bufferOffset, length);
        }

        /// <summary>
        /// Returns the GUID value of the specified field.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The GUID value of the specified field.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        /// <exception cref="IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="IDataRecord.FieldCount"/>.</exception>
        /// <exception cref="InvalidCastException">The specified cast is not valid.</exception>
        public Guid GetGuid(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);

            return this.context.Buffers[i].Guid;
        }

        /// <summary>
        /// Gets the 16-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The 16-bit signed integer value of the specified field.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        /// <exception cref="IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="IDataRecord.FieldCount"/>.</exception>
        /// <exception cref="InvalidCastException">The specified cast is not valid.</exception>
        public short GetInt16(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);

            return this.context.Buffers[i].Int16;
        }

        /// <summary>
        /// Gets the 32-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The 32-bit signed integer value of the specified field.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        /// <exception cref="IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="IDataRecord.FieldCount"/>.</exception>
        /// <exception cref="InvalidCastException">The specified cast is not valid.</exception>
        public int GetInt32(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);

            return this.context.Buffers[i].Int32;
        }

        /// <summary>
        /// Gets the 64-bit signed integer value of the specified field.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The 64-bit signed integer value of the specified field.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        /// <exception cref="IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="IDataRecord.FieldCount"/>.</exception>
        /// <exception cref="InvalidCastException">The specified cast is not valid.</exception>
        public long GetInt64(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);

            return this.context.Buffers[i].Int64;
        }

        /// <summary>
        /// Gets the single-precision floating point number of the specified field.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The single-precision floating point number of the specified field.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        /// <exception cref="IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="IDataRecord.FieldCount"/>.</exception>
        /// <exception cref="InvalidCastException">The specified cast is not valid.</exception>
        public float GetFloat(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);

            return this.context.Buffers[i].Float;
        }

        /// <summary>
        /// Gets the double-precision floating point number of the specified field.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The double-precision floating point number of the specified field.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        /// <exception cref="IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="IDataRecord.FieldCount"/>.</exception>
        /// <exception cref="InvalidCastException">The specified cast is not valid.</exception>
        public double GetDouble(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);

            return this.context.Buffers[i].Double;
        }

        /// <summary>
        /// Gets the string value of the specified field.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The string value of the specified field.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        /// <exception cref="IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="IDataRecord.FieldCount"/>.</exception>
        /// <exception cref="InvalidCastException">The specified cast is not valid.</exception>
        public string GetString(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);

            return this.context.Buffers[i].String;
        }

        /// <summary>
        /// Gets the fixed-position numeric value of the specified field.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The fixed-position numeric value of the specified field.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        /// <exception cref="IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="IDataRecord.FieldCount"/>.</exception>
        /// <exception cref="InvalidCastException">The specified cast is not valid.</exception>
        public decimal GetDecimal(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);

            return this.context.Buffers[i].Decimal;
        }

        /// <summary>
        /// Gets the date and time data value of the specified field.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The date and time data value of the specified field.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        /// <exception cref="IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="IDataRecord.FieldCount"/>.</exception>
        /// <exception cref="InvalidCastException">The specified cast is not valid.</exception>
        public DateTime GetDateTime(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);

            return this.context.Buffers[i].DateTime;
        }

        /// <summary>
        /// Gets the time span value of the specified field.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>The time span value of the specified field.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        /// <exception cref="IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="IDataRecord.FieldCount"/>.</exception>
        /// <exception cref="InvalidCastException">The specified cast is not valid.</exception>
        public TimeSpan GetTimeSpan(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);

            return this.context.Buffers[i].TimeSpan;
        }

        /// <summary>
        /// Returns an <see cref="IDataReader"/> for the specified column ordinal.
        /// </summary>
        /// <param name="i">The index of the field to find.</param>
        /// <returns>An <see cref="IDataReader"/>.</returns>
        /// <remarks>The <see cref="ProtoDataReader"/> does not support <see cref="IDataRecord.GetData(int)"/> and always throws a <see cref="NotSupportedException"/>.</remarks>
        IDataReader IDataRecord.GetData(int i)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Return whether the specified field is set to null.
        /// </summary>
        /// <param name="i">The zero-based column ordinal.</param>
        /// <returns>true if the specified field is set to null; otherwise, false.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        /// <exception cref="IndexOutOfRangeException">The index passed was outside the range of 0 through <see cref="IDataRecord.FieldCount"/>.</exception>
        public bool IsDBNull(int i)
        {
            this.ThrowIfClosed();
            this.ThrowIfNoData();
            this.ThrowIfIndexOutOfRange(i);

            return this.context.Buffers[i].IsNull;
        }

        /// <summary>
        /// Closes the <see cref="ProtoDataReader"/> object.
        /// </summary>
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

        /// <summary>
        /// Advances the data reader to the next result.
        /// </summary>
        /// <returns>true if there are more result sets; otherwise false.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        public bool NextResult()
        {
            this.ThrowIfClosed();

            this.ConsumeAnyRemainingRows();

            this.schemaTable = null;
            this.context.Columns = new List<ProtoDataColumn>();

            return ResultReader.ReadResult(this.context);
        }

        /// <summary>
        /// Returns a <see cref="DataTable"/> that describes the column metadata of the <see cref="ProtoDataReader"/>.
        /// </summary>
        /// <returns>A <see cref="DataTable"/> that describes the column metadata.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        public DataTable GetSchemaTable()
        {
            this.ThrowIfClosed();

            if (this.schemaTable == null)
            {
                this.schemaTable = this.BuildSchemaTable();
            }

            return this.schemaTable;
        }

        /// <summary>
        /// Advances the <see cref="ProtoDataReader"/> to the next record.
        /// </summary>
        /// <returns>true if there are more rows; otherwise false.</returns>
        /// <exception cref="InvalidOperationException">The <see cref="ProtoDataReader"/> is closed.</exception>
        public bool Read()
        {
            this.ThrowIfClosed();

            return RecordReader.ReadRecord(this.context);
        }

        /// <summary>
        /// Releases all resources used by the current instance of the <see cref="ProtoDataReader"/> class.
        /// </summary>
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
    }
}