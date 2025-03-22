// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Runtime.InteropServices;

namespace ProtoBuf.Data.Internal
{
    internal sealed class ProtoDataBuffer
    {
        private ValueTypeBuffer valueTypeBuffer;
        private BufferType bufferType;
        private object referenceTypeBuffer;

        public ProtoDataBuffer()
        {
            this.IsNull = true;
        }

        private enum BufferType
        {
            Empty = 0,
            String = 1,
            DateTime = 2,
            Int32 = 3,
            Int64 = 4,
            Int16 = 5,
            Boolean = 6,
            Byte = 7,
            Float = 8,
            Double = 9,
            Guid = 10,
            Char = 11,
            Decimal = 12,
            ByteArray = 13,
            CharArray = 14,
            TimeSpan = 15,
            DateTimeOffset = 16
        }

        public bool IsNull { get; set; }

        public bool Boolean
        {
            get
            {
                this.ThrowIfNull();
                this.ThrowIfInvalidBufferType(BufferType.Boolean);

                return this.valueTypeBuffer.Boolean;
            }

            set
            {
                this.bufferType = BufferType.Boolean;

                this.valueTypeBuffer.Boolean = value;
            }
        }

        public byte Byte
        {
            get
            {
                this.ThrowIfNull();
                this.ThrowIfInvalidBufferType(BufferType.Byte);

                return this.valueTypeBuffer.Byte;
            }

            set
            {
                this.bufferType = BufferType.Byte;

                this.valueTypeBuffer.Byte = value;
            }
        }

        public byte[] ByteArray
        {
            get
            {
                this.ThrowIfNull();
                this.ThrowIfInvalidBufferType(BufferType.ByteArray);

                return (byte[])this.referenceTypeBuffer;
            }

            set
            {
                this.bufferType = BufferType.ByteArray;

                this.referenceTypeBuffer = value;
            }
        }

        public char Char
        {
            get
            {
                this.ThrowIfNull();
                this.ThrowIfInvalidBufferType(BufferType.Char);

                return this.valueTypeBuffer.Char;
            }

            set
            {
                this.bufferType = BufferType.Char;

                this.valueTypeBuffer.Char = value;
            }
        }

        public char[] CharArray
        {
            get
            {
                this.ThrowIfNull();
                this.ThrowIfInvalidBufferType(BufferType.CharArray);

                return (char[])this.referenceTypeBuffer;
            }

            set
            {
                this.bufferType = BufferType.CharArray;

                this.referenceTypeBuffer = value;
            }
        }

        public DateTime DateTime
        {
            get
            {
                this.ThrowIfNull();
                this.ThrowIfInvalidBufferType(BufferType.DateTime);

                return this.valueTypeBuffer.DateTime;
            }

            set
            {
                this.bufferType = BufferType.DateTime;

                this.valueTypeBuffer.DateTime = value;
            }
        }

        public decimal Decimal
        {
            get
            {
                this.ThrowIfNull();
                this.ThrowIfInvalidBufferType(BufferType.Decimal);

                return this.valueTypeBuffer.Decimal;
            }

            set
            {
                this.bufferType = BufferType.Decimal;

                this.valueTypeBuffer.Decimal = value;
            }
        }

        public double Double
        {
            get
            {
                this.ThrowIfNull();
                this.ThrowIfInvalidBufferType(BufferType.Double);

                return this.valueTypeBuffer.Double;
            }

            set
            {
                this.bufferType = BufferType.Double;

                this.valueTypeBuffer.Double = value;
            }
        }

        public float Float
        {
            get
            {
                this.ThrowIfNull();
                this.ThrowIfInvalidBufferType(BufferType.Float);

                return this.valueTypeBuffer.Float;
            }

            set
            {
                this.bufferType = BufferType.Float;

                this.valueTypeBuffer.Float = value;
            }
        }

        public Guid Guid
        {
            get
            {
                this.ThrowIfNull();
                this.ThrowIfInvalidBufferType(BufferType.Guid);

                return this.valueTypeBuffer.Guid;
            }

            set
            {
                this.bufferType = BufferType.Guid;

                this.valueTypeBuffer.Guid = value;
            }
        }

        public int Int32
        {
            get
            {
                this.ThrowIfNull();
                this.ThrowIfInvalidBufferType(BufferType.Int32);

                return this.valueTypeBuffer.Int32;
            }

            set
            {
                this.bufferType = BufferType.Int32;

                this.valueTypeBuffer.Int32 = value;
            }
        }

        public long Int64
        {
            get
            {
                this.ThrowIfNull();
                this.ThrowIfInvalidBufferType(BufferType.Int64);

                return this.valueTypeBuffer.Int64;
            }

            set
            {
                this.bufferType = BufferType.Int64;

                this.valueTypeBuffer.Int64 = value;
            }
        }

        public short Int16
        {
            get
            {
                this.ThrowIfNull();
                this.ThrowIfInvalidBufferType(BufferType.Int16);

                return this.valueTypeBuffer.Int16;
            }

            set
            {
                this.bufferType = BufferType.Int16;

                this.valueTypeBuffer.Int16 = value;
            }
        }

        public string String
        {
            get
            {
                this.ThrowIfNull();
                this.ThrowIfInvalidBufferType(BufferType.String);

                return (string)this.referenceTypeBuffer;
            }

            set
            {
                this.bufferType = BufferType.String;

                this.referenceTypeBuffer = value;
            }
        }

        public TimeSpan TimeSpan
        {
            get
            {
                this.ThrowIfNull();
                this.ThrowIfInvalidBufferType(BufferType.TimeSpan);

                return this.valueTypeBuffer.TimeSpan;
            }

            set
            {
                this.bufferType = BufferType.TimeSpan;

                this.valueTypeBuffer.TimeSpan = value;
            }
        }

        public DateTimeOffset DateTimeOffset
        {
            get
            {
                this.ThrowIfNull();
                this.ThrowIfInvalidBufferType(BufferType.DateTimeOffset);

                return this.valueTypeBuffer.DateTimeOffset;
            }

            set
            {
                this.bufferType = BufferType.DateTimeOffset;

                this.valueTypeBuffer.DateTimeOffset = value;
            }
        }

        public object Value
        {
            get
            {
                if (this.IsNull)
                {
                    return DBNull.Value;
                }

                switch (this.bufferType)
                {
                    case BufferType.Empty: return DBNull.Value;
                    case BufferType.Boolean: return this.Boolean;
                    case BufferType.Byte: return this.Byte;
                    case BufferType.ByteArray: return this.ByteArray;
                    case BufferType.Char: return this.Char;
                    case BufferType.CharArray: return this.CharArray;
                    case BufferType.DateTime: return this.DateTime;
                    case BufferType.Decimal: return this.Decimal;
                    case BufferType.Double: return this.Double;
                    case BufferType.Float: return this.Float;
                    case BufferType.Guid: return this.Guid;
                    case BufferType.Int32: return this.Int32;
                    case BufferType.Int64: return this.Int64;
                    case BufferType.Int16: return this.Int16;
                    case BufferType.String: return this.String;
                    case BufferType.TimeSpan: return this.TimeSpan;
                    case BufferType.DateTimeOffset: return this.DateTimeOffset;
                }

                return null;
            }
        }

        public static void Clear(ProtoDataBuffer[] buffers)
        {
            for (var i = 0; i < buffers.Length; i++)
            {
                buffers[i].Clear();
            }
        }

        public static void Initialize(ProtoDataBuffer[] buffers)
        {
            for (var i = 0; i < buffers.Length; i++)
            {
                buffers[i] = new ProtoDataBuffer();
            }
        }

        private void Clear()
        {
            this.referenceTypeBuffer = null;
            this.IsNull = true;
            this.bufferType = BufferType.Empty;
        }

        private void ThrowIfNull()
        {
            if (this.IsNull)
            {
                throw new InvalidOperationException("Data is Null. This method or property cannot be called on Null values.");
            }
        }

        private void ThrowIfInvalidBufferType(BufferType expectedBufferType)
        {
            if (this.bufferType != expectedBufferType)
            {
                throw new InvalidOperationException($"Invalid attempt to read data of type '{expectedBufferType}' when data is of type '{this.bufferType}'.");
            }
        }

        [StructLayout(LayoutKind.Explicit)]
        private struct ValueTypeBuffer
        {
            [FieldOffset(0)]
            public bool Boolean;
            [FieldOffset(0)]
            public byte Byte;
            [FieldOffset(0)]
            public char Char;
            [FieldOffset(0)]
            public DateTime DateTime;
            [FieldOffset(0)]
            public decimal Decimal;
            [FieldOffset(0)]
            public double Double;
            [FieldOffset(0)]
            public float Float;
            [FieldOffset(0)]
            public Guid Guid;
            [FieldOffset(0)]
            public int Int32;
            [FieldOffset(0)]
            public long Int64;
            [FieldOffset(0)]
            public short Int16;
            [FieldOffset(0)]
            public TimeSpan TimeSpan;
            [FieldOffset(0)]
            public DateTimeOffset DateTimeOffset;
        }
    }
}
