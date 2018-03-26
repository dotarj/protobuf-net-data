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
        private readonly ProtoDataColumnFactory columnFactory;

        private IDataReader reader;
        private ProtoWriter writer;
        private CircularStream bufferStream;
        private bool disposed;
        private int resultIndex;
        private bool isHeaderWritten;
        private RowWriter rowWriter;
        private SubItemToken currentResultToken;
        private bool readerIsClosed;

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
            this.columnFactory = new ProtoDataColumnFactory();
            this.bufferStream = new CircularStream(bufferSize);
            this.writer = new ProtoWriter(this.bufferStream, null, null);
        }

        ~ProtoDataStream()
        {
            this.Dispose(false);
        }

        public override bool CanRead
        {
            get { return !this.disposed; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return false; }
        }

        public override long Length
        {
            get { return -1; }
        }

        public override long Position
        {
            get
            {
                if (this.readerIsClosed)
                {
                    throw new InvalidOperationException("Reader is closed.");
                }

                return this.bufferStream.Position;
            }

            set
            {
                throw new InvalidOperationException("Cannot set stream position.");
            }
        }

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new InvalidOperationException("This stream cannot seek.");
        }

        public override void SetLength(long value)
        {
            throw new InvalidOperationException();
        }

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

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new InvalidOperationException("This is a stream for reading serialized bytes. Writing is not supported.");
        }

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

            this.currentResultToken = ProtoWriter.StartSubItem(this.resultIndex, this.writer);

            IList<ProtoDataColumn> columns = this.columnFactory.GetColumns(this.reader, this.options);
            new HeaderWriter(this.writer).WriteHeader(columns);

            this.rowWriter = new RowWriter(this.writer, columns, this.options);

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
                    this.rowWriter.WriteRow(this.reader);
                }
                else
                {
                    this.resultIndex++;
                    ProtoWriter.EndSubItem(this.currentResultToken, this.writer);

                    if (this.reader.NextResult())
                    {
                        // Start next data table.
                        this.isHeaderWritten = false;
                        this.FillBuffer(requestedLength);
                    }
                    else
                    {
                        // All done, no more results.
                        // little optimization
                        this.writer.Close();
                        this.CloseReader();
                    }

                    break;
                }
            }
        }
    }
}