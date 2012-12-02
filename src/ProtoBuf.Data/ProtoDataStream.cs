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

            FillBuffer(count);
            return bufferStream.Read(buffer, offset, count);
        }

        private void FillBuffer(int requestedLength)
        {
            // only supports 1 data table currently

            ProtoWriter.WriteFieldHeader(1, WireType.StartGroup, writer);

            currentResultToken = ProtoWriter.StartSubItem(resultIndex, writer);

            var columns = new ProtoDataColumnFactory().GetColumns(reader, options);

            new HeaderWriter(writer).WriteHeader(columns);

            var rowWriter = new RowWriter(writer, columns, options);

            // write the rows
            while (reader.Read())
                rowWriter.WriteRow(reader);

            ProtoWriter.EndSubItem(currentResultToken, writer);

            resultIndex++;

            readerIsClosed = true;
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

        protected override void Dispose(bool disposing)
        {
            if (writer != null)
            {
                writer.Close();
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
    }
}