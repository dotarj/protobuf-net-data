// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using ProtoBuf.Data.Internal;

namespace ProtoBuf.Data
{
    /// <summary>
    /// Serializes an <see cref="System.Data.IDataReader"/> to a binary stream
    /// which can be read (it serializes additional rows with subsequent calls
    /// to <see cref="Read"/>). Useful for scenarios like WCF where you cannot
    /// write to the output stream directly.
    /// </summary>
    /// <remarks>Not guaranteed to be thread safe.</remarks>
    public class ProtoDataStream : Stream
    {
        /// <summary>
        /// Buffer size.
        /// </summary>
        public const int DefaultBufferSize = 128 * ProtoWriterBufferSize;

        private const int ProtoWriterBufferSize = 1024;

        private readonly ProtoDataWriterOptions options;

        private IDataReader reader;
        private ProtoWriter writer;
        private CircularStream bufferStream;
        private bool disposed;
        private int resultIndex;
        private bool isHeaderWritten;
        private ProtoWriterContext context;
        private bool readerIsClosed;
        private int recordIndex;

        /// <summary>
        /// Initializes a new instance of the <see cref="ProtoDataStream"/> class.
        /// </summary>
        /// <param name="dataSet">The <see cref="DataSet"/>who's contents to serialize.</param>
        /// <param name="bufferSize">Buffer size to use when serializing rows.
        /// You should not need to change this unless you have exceptionally
        /// large rows or an exceptionally high number of columns.</param>
        public ProtoDataStream(
            DataSet dataSet, int bufferSize = DefaultBufferSize)
            : this(dataSet.CreateDataReader(), new ProtoDataWriterOptions(), bufferSize)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ProtoDataStream"/> class.
        /// </summary>
        /// <param name="dataSet">The <see cref="DataSet"/>who's contents to serialize.</param>
        /// <param name="options"><see cref="ProtoDataWriterOptions"/> specifying any custom serialization options.</param>
        /// <param name="bufferSize">Buffer size to use when serializing rows.
        /// You should not need to change this unless you have exceptionally
        /// large rows or an exceptionally high number of columns.</param>
        public ProtoDataStream(
            DataSet dataSet,
            ProtoDataWriterOptions options,
            int bufferSize = DefaultBufferSize)
            : this(dataSet.CreateDataReader(), options, bufferSize)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ProtoDataStream"/> class.
        /// </summary>
        /// <param name="dataTable">The <see cref="DataTable"/>who's contents to serialize.</param>
        /// <param name="bufferSize">Buffer size to use when serializing rows.
        /// You should not need to change this unless you have exceptionally
        /// large rows or an exceptionally high number of columns.</param>
        public ProtoDataStream(
            DataTable dataTable, int bufferSize = DefaultBufferSize)
            : this(dataTable.CreateDataReader(), new ProtoDataWriterOptions(), bufferSize)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ProtoDataStream"/> class.
        /// </summary>
        /// <param name="dataTable">The <see cref="DataTable"/>who's contents to serialize.</param>
        /// <param name="options"><see cref="ProtoDataWriterOptions"/> specifying any custom serialization options.</param>
        /// <param name="bufferSize">Buffer size to use when serializing rows.
        /// You should not need to change this unless you have exceptionally
        /// large rows or an exceptionally high number of columns.</param>
        public ProtoDataStream(
            DataTable dataTable,
            ProtoDataWriterOptions options,
            int bufferSize = DefaultBufferSize)
            : this(dataTable.CreateDataReader(), options, bufferSize)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ProtoDataStream"/> class.
        /// </summary>
        /// <param name="reader">The <see cref="IDataReader"/>who's contents to serialize.</param>
        /// <param name="bufferSize">Buffer size to use when serializing rows.
        /// You should not need to change this unless you have exceptionally
        /// large rows or an exceptionally high number of columns.</param>
        public ProtoDataStream(IDataReader reader, int bufferSize = DefaultBufferSize)
            : this(reader, new ProtoDataWriterOptions(), bufferSize)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ProtoDataStream"/> class.
        /// </summary>
        /// <param name="reader">The <see cref="IDataReader"/>who's contents to serialize.</param>
        /// <param name="options"><see cref="ProtoDataWriterOptions"/> specifying any custom serialization options.</param>
        /// <param name="bufferSize">Buffer size to use when serializing rows.
        /// You should not need to change this unless you have exceptionally
        /// large rows or an exceptionally high number of columns.</param>
        public ProtoDataStream(
            IDataReader reader,
            ProtoDataWriterOptions options,
            int bufferSize = DefaultBufferSize)
        {
            if (reader == null)
            {
                throw new ArgumentNullException("reader");
            }

            if (options == null)
            {
                throw new ArgumentNullException("options");
            }

            this.reader = reader;
            this.options = options;

            this.resultIndex = 0;
            this.bufferStream = new CircularStream(bufferSize);
            this.writer = ProtoWriter.Create(this.bufferStream, null, null);
            this.context = new ProtoWriterContext(this.writer, this.options);
        }

        /// <summary>
        /// Finalizes an instance of the <see cref="ProtoDataStream"/> class.
        /// </summary>
        ~ProtoDataStream()
        {
            this.Dispose(false);
        }

        /// <summary>
        /// Gets a value indicating whether the <see cref="ProtoDataStream"/> supports reading.
        /// </summary>
        /// <returns>true if data can be read from the stream; otherwise, false.</returns>
        public override bool CanRead
        {
            get { return !this.disposed; }
        }

        /// <summary>
        /// Gets a value indicating whether the stream supports writing. This property is not currently supported and always returns false.
        /// </summary>
        /// <returns>false in all cases to indicate that <see cref="ProtoDataStream"/> cannot write to the stream.</returns>
        public override bool CanSeek
        {
            get { return false; }
        }

        /// <summary>
        /// Gets a value indicating whether the stream supports seeking. This property is not currently supported and always returns false.
        /// </summary>
        /// <returns>false in all cases to indicate that <see cref="ProtoDataStream"/> cannot seek a specific location in the stream.</returns>
        public override bool CanWrite
        {
            get { return false; }
        }

        /// <summary>
        /// Gets the length of the data available on the stream. This property is not currently supported and always throws a <see cref="NotSupportedException"/>.
        /// </summary>
        /// <returns>The length of the data available on the stream.</returns>
        /// <exception cref="NotSupportedException">Any use of this property.</exception>
        public override long Length
        {
            get { throw new NotSupportedException(); }
        }

        /// <summary>
        /// Gets or sets the current position in the stream. This set operation of this property is not currently supported and always throws a  <see cref="NotSupportedException"/>.
        /// </summary>
        /// <returns>The current position in the stream.</returns>
        /// <exception cref="InvalidOperationException">The underlying <see cref="ProtoDataReader"/> is closed.</exception>
        /// <exception cref="NotSupportedException">Any use of the set operation of this property.</exception>
        public override long Position
        {
            get
            {
                if (this.readerIsClosed)
                {
                    throw new InvalidOperationException("Invalid attempt to call method when underlying reader is closed.");
                }

                return this.bufferStream.Position;
            }

            set
            {
                throw new NotSupportedException();
            }
        }

        /// <summary>
        /// Clears all buffers for this stream and causes any buffered data to be written to the underlying device.
        /// </summary>
        public override void Flush()
        {
        }

        /// <summary>
        /// Sets the current position of the stream to the given value. This method is not currently supported and always throws a <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="offset">A byte offset relative to the origin parameter.</param>
        /// <param name="origin">A value of type <see cref="SeekOrigin"/> indicating the reference point used to obtain the new position.</param>
        /// <returns>The new position within the current stream.</returns>
        /// <exception cref="NotSupportedException">Any use of this method.</exception>
        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Sets the length of the current stream. This method is not currently supported and always throws a <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="value">The desired length of the current stream in bytes.</param>
        /// <exception cref="NotSupportedException">Any use of this method.</exception>
        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Reads data from the <see cref="ProtoDataStream"/>.
        /// </summary>
        /// <param name="buffer">An array of type <see cref="byte"/> that is the location in memory to store data read from the <see cref="ProtoDataStream"/>.</param>
        /// <param name="offset">The location in buffer to begin storing the data to.</param>
        /// <param name="count">The number of bytes to read from the <see cref="ProtoDataStream"/>.</param>
        /// <returns>The number of bytes read from the <see cref="ProtoDataStream"/>.</returns>
        public override int Read(byte[] buffer, int offset, int count)
        {
            if (this.bufferStream.Length == 0 && this.readerIsClosed)
            {
                return 0;
            }

            if (!this.readerIsClosed)
            {
                this.FillBuffer(count);
            }

            return this.bufferStream.Read(buffer, offset, count);
        }

        /// <summary>
        /// Writes data to the <see cref="ProtoDataStream"/>. This method is not currently supported and always throws a <see cref="NotSupportedException"/>.
        /// </summary>
        /// <param name="buffer">An array of type <see cref="byte"/> that contains the data to write to the <see cref="ProtoDataStream"/>.</param>
        /// <param name="offset">The location in buffer from which to start writing data.</param>
        /// <param name="count">The number of bytes to write to the <see cref="ProtoDataStream"/>.</param>
        /// <exception cref="NotSupportedException">Any use of this method.</exception>
        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Releases the unmanaged resources used by the <see cref="ProtoDataStream"/> and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
        protected override void Dispose(bool disposing)
        {
            if (!this.disposed)
            {
                if (disposing)
                {
                    this.CloseReader();

                    if (this.writer != null)
                    {
                        ((IDisposable)this.writer).Dispose();
                        this.writer = null;
                    }

                    if (this.bufferStream != null)
                    {
                        this.bufferStream.Dispose();
                        this.bufferStream = null;
                    }
                }

                this.disposed = true;
            }
        }

        private void CloseReader()
        {
            if (this.reader != null)
            {
                this.reader.Dispose();
                this.reader = null;
            }

            this.readerIsClosed = true;
        }

        private void WriteHeaderIfRequired()
        {
            if (this.isHeaderWritten)
            {
                return;
            }

            ProtoWriter.WriteFieldHeader(1, WireType.StartGroup, this.writer);

            this.context.StartSubItem(this.resultIndex);

            this.context.Columns = ProtoDataColumnFactory.GetColumns(this.reader, this.options);

            ColumnsWriter.WriteColumns(this.context);

            this.isHeaderWritten = true;
        }

        private void FillBuffer(int requestedLength)
        {
            // Only supports 1 data table currently.
            this.WriteHeaderIfRequired();

            // write the rows
            // protobuf-net not always return 1024 byte, so buffer can owerflow
            while (this.bufferStream.Length < requestedLength && this.bufferStream.Capacity - this.bufferStream.Length >= ProtoWriterBufferSize)
            {
                // NB protobuf-net only flushes every 1024 bytes. So
                // it might take a few iterations for bufferStream.Length to
                // see any change.
                if (this.reader.Read())
                {
                    RecordWriter.WriteRecord(this.context, this.recordIndex, this.reader);
                }
                else
                {
                    this.resultIndex++;

                    this.context.EndSubItem();

                    if (this.reader.NextResult())
                    {
                        this.isHeaderWritten = false;
                        this.recordIndex = 0;

                        this.FillBuffer(requestedLength);
                    }
                    else
                    {
                        this.writer.Close();
                        this.CloseReader();
                    }

                    break;
                }
            }
        }
    }
}