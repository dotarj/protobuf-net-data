using System;
using System.Data;
using System.IO;
using ProtoBuf.Data.Internal;

namespace ProtoBuf.Data
{
    public class ProtoDataStream : Stream
    {
        private IDataReader reader;
        private readonly ProtoDataWriterOptions options;
        private ProtoWriter writer;
        private int resultIndex;
        private SubItemToken currentResultToken;
        private Stream bufferStream;
        private bool readerIsClosed;
        private readonly ProtoDataColumnFactory columnFactory;

        public ProtoDataStream(DataTable dataTable)
            : this(dataTable.CreateDataReader(), new ProtoDataWriterOptions()) { }

        public ProtoDataStream(DataTable dataTable, ProtoDataWriterOptions options)
            : this(dataTable.CreateDataReader(), options) { }

        public ProtoDataStream(IDataReader reader)
            : this(reader, new ProtoDataWriterOptions()) { }

        public ProtoDataStream(IDataReader reader, ProtoDataWriterOptions options)
        {
            if (reader == null) throw new ArgumentNullException("reader");
            if (options == null) throw new ArgumentNullException("options");
            this.reader = reader;
            this.options = options;

            resultIndex = 0;
            columnFactory = new ProtoDataColumnFactory();
            bufferStream = new WriteAheadReadBufferStream();
            writer = new ProtoWriter(bufferStream, null, null);
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
            if (bufferStream.Length == 0 && readerIsClosed)
                return 0;

            if (!readerIsClosed)
                FillBuffer(count);

            return bufferStream.Read(buffer, offset, count);
        }

        private bool isHeaderWritten;
        private RowWriter rowWriter;

        private void WriteHeaderIfRequired()
        {
            if (isHeaderWritten)
                return;

            ProtoWriter.WriteFieldHeader(1, WireType.StartGroup, writer);

            currentResultToken = ProtoWriter.StartSubItem(resultIndex, writer);

            var columns = columnFactory.GetColumns(reader, options);
            new HeaderWriter(writer).WriteHeader(columns);

            rowWriter = new RowWriter(writer, columns, options);

            isHeaderWritten = true;
        }

        private void FillBuffer(int requestedLength)
        {
            // only supports 1 data table currently

            WriteHeaderIfRequired();

            // write the rows
            while (bufferStream.Length < requestedLength)
            {
                // NB protobuf-net only flushes every 1024 bytes. So
                // it might take a few iterations for bufferStream.Length to
                // see any change.
                if (reader.Read())
                    rowWriter.WriteRow(reader);
                else
                {
                    resultIndex++;
                    ProtoWriter.EndSubItem(currentResultToken, writer);

                    if (reader.NextResult())
                    {
                        // Start next data table.
                        isHeaderWritten = false;
                        FillBuffer(requestedLength);
                    }
                    else
                    {
                        // All done, no more results.
                        writer.Close();
                        Close();
                    }
                    
                    break;
                }
            }
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new InvalidOperationException("This is a stream for reading serialized bytes. Writing is not supported.");
        }

        public override bool CanRead
        {
            get { return true; }
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
                return bufferStream.Position;
            }
            set
            {
                throw new InvalidOperationException("Cannot set stream position.");
            }
        }

        public override void Close()
        {
            readerIsClosed = true;
            if (reader != null)
                reader.Close();
        }

        protected override void Dispose(bool disposing)
        {
            if (!disposed)
            {
                if (disposing)
                {
                    if (writer != null)
                    {
                        ((IDisposable)writer).Dispose();
                        writer = null;
                    }

                    if (reader != null)
                    {
                        reader.Dispose();
                        reader = null;
                    }

                    if (bufferStream != null)
                    {
                        bufferStream.Dispose();
                        bufferStream = null;
                    }
                }

                disposed = true;
            }
        }

        ~ProtoDataStream()
        {
            Dispose(false);
        }

        private bool disposed;
    }
}