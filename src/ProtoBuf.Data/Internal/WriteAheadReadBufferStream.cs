using System;
using System.Collections.Generic;
using System.IO;

namespace ProtoBuf.Data.Internal
{
    /// <summary>
    /// A stream that can be written to (ahead) and read to.
    /// </summary>
    internal class WriteAheadReadBufferStream : Stream
    {
        private int position;
        readonly LinkedList<byte[]> buffers = new LinkedList<byte[]>();
        private long length;

        public override void Write(byte[] buffer, int offset, int count)
        {
            var buf = new byte[count];
            Array.Copy(buffer, offset, buf, 0, count);
            length += count;
            buffers.AddLast(buf);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (buffer == null) throw new ArgumentNullException("buffer");

            if (HasNoMoreBuffersAvailable())
                return 0;

            if (count == 0)
                return 0;

            byte[] currentBuffer;

            var totalNumberOfBytesRead = 0;
            int numberOfBytesReadFromCurrentBuffer;
            do
            {
                currentBuffer = GetCurrentBuffer();

                numberOfBytesReadFromCurrentBuffer = DoRead(buffer, 
                    count - totalNumberOfBytesRead, currentBuffer, offset + totalNumberOfBytesRead);

                totalNumberOfBytesRead += numberOfBytesReadFromCurrentBuffer;

            } while (totalNumberOfBytesRead < count && !HasNoMoreBuffersAvailable());

            PutLeftoverBytesAtFrontOfQueue(currentBuffer, numberOfBytesReadFromCurrentBuffer);

            length -= totalNumberOfBytesRead;
            position += totalNumberOfBytesRead;

            return totalNumberOfBytesRead;
        }

        // Check if caller didn't have enough space to fit the buffer.
        // Put the remaining bytes at the front of the queue.
        private void PutLeftoverBytesAtFrontOfQueue(byte[] currentBuffer, int numberOfBytesRead)
        {
            if (currentBuffer == null) throw new ArgumentNullException("currentBuffer");

            if (numberOfBytesRead == currentBuffer.Length)
                return; // Clean read!

            var remainingBuffer = new byte[currentBuffer.Length - numberOfBytesRead];
            Array.Copy(currentBuffer, numberOfBytesRead, remainingBuffer, 0, remainingBuffer.Length);

            buffers.AddFirst(remainingBuffer);
        }

        private static int DoRead(byte[] buffer, int count, byte[] currentBuffer, int offset)
        {
            var maxNumberOfBytesWeCanWrite = Math.Min(count, currentBuffer.Length);

            Array.Copy(currentBuffer, 0, buffer, offset, maxNumberOfBytesWeCanWrite);

            return maxNumberOfBytesWeCanWrite;
        }

        private bool HasNoMoreBuffersAvailable()
        {
            return buffers.Count == 0;
        }

        private byte[] GetCurrentBuffer()
        {
            var currentBuffer = buffers.First.Value;
            buffers.RemoveFirst();
            return currentBuffer;
        }

        public override long Length
        {
            get { return length; }
        }

        // Read position
        public override long Position
        {
            get { return position; }
            set { throw new InvalidOperationException("Cannot set position."); }
        }

        public override bool CanRead
        {
            get { return false; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return true; }
        }

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new InvalidOperationException();
        }

        public override void SetLength(long value)
        {
            throw new InvalidOperationException();
        }
    }
}