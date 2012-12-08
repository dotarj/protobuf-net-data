// Copyright (c) 2012, Alex Regueiro http://circularbuffer.codeplex.com
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without 
// modification, are permitted provided that the following conditions are met:
//
// * Redistributions of source code must retain the above copyright notice, 
//   this list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
//   this list of conditions and the following disclaimer in the documentation
//   and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

using System;
using System.IO;

namespace ProtoBuf.Data.Internal
{
    internal class CircularStream : Stream
    {
        private CircularBuffer<byte> buffer;

        public CircularStream(int bufferCapacity)
            : base()
        {
            buffer = new CircularBuffer<byte>(bufferCapacity);
        }

        public override long Position
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public int Capacity
        {
            get { return buffer.Capacity; }
            set { buffer.Capacity = value; }
        }

        public override long Length
        {
            get { return buffer.Size; }
        }

        public override bool CanSeek
        {
            get { return true; }
        }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanWrite
        {
            get { return true; }
        }

        public byte[] GetBuffer()
        {
            return buffer.GetBuffer();
        }

        public byte[] ToArray()
        {
            return buffer.ToArray();
        }

        public override void Flush()
        {
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            this.buffer.Put(buffer, offset, count);
        }

        public override void WriteByte(byte value)
        {
            this.buffer.Put(value);
        }
        
        public override int Read(byte[] buffer, int offset, int count)
        {
            return this.buffer.Get(buffer, offset, count);
        }

        public override int ReadByte()
        {
            return this.buffer.Get();
        }
        
        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }
    }
}