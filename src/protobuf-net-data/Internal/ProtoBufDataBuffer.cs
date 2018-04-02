// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Runtime.InteropServices;

namespace ProtoBuf.Data.Internal
{
    internal sealed class ProtoBufDataBuffer
    {
        private ValueTypeBuffer valueTypeBuffer;
        private BufferType bufferType;
        private object referenceTypeBuffer;

        public ProtoBufDataBuffer()
        {
            this.IsNull = true;
        }

        private enum BufferType
        {
            Empty = 0,
            String = 1,
            DateTime = 2,
            Int = 3,
            Long = 4,
            Short = 5,
            Bool = 6,
            Byte = 7,
            Float = 8,
            Double = 9,
            Guid = 10,
            Char = 11,
            Decimal = 12,
            ByteArray = 13,
            CharArray = 14,
            TimeSpan = 15
        }

        public bool IsNull { get; set; }

        public bool Bool
        {
            get
            {
                this.ThrowIfNull();

                if (this.bufferType == BufferType.Bool)
                {
                    return this.valueTypeBuffer.Bool;
                }

                return (bool)this.Value;
            }

            set
            {
                this.bufferType = BufferType.Bool;

                this.valueTypeBuffer.Bool = value;
            }
        }

        public byte Byte
        {
            get
            {
                this.ThrowIfNull();

                if (this.bufferType == BufferType.Byte)
                {
                    return this.valueTypeBuffer.Byte;
                }

                return (byte)this.Value;
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

                if (this.bufferType == BufferType.ByteArray)
                {
                    return (byte[])this.referenceTypeBuffer;
                }

                return (byte[])this.Value;
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

                if (this.bufferType == BufferType.Char)
                {
                    return this.valueTypeBuffer.Char;
                }

                return (char)this.Value;
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

                if (this.bufferType == BufferType.CharArray)
                {
                    return (char[])this.referenceTypeBuffer;
                }

                return (char[])this.Value;
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

                if (this.bufferType == BufferType.DateTime)
                {
                    return this.valueTypeBuffer.DateTime;
                }

                return (DateTime)this.Value;
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

                if (this.bufferType == BufferType.Decimal)
                {
                    return this.valueTypeBuffer.Decimal;
                }

                return (decimal)this.Value;
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

                if (this.bufferType == BufferType.Double)
                {
                    return this.valueTypeBuffer.Double;
                }

                return (double)this.Value;
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

                if (this.bufferType == BufferType.Float)
                {
                    return this.valueTypeBuffer.Float;
                }

                return (float)this.Value;
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

                if (this.bufferType == BufferType.Guid)
                {
                    return this.valueTypeBuffer.Guid;
                }

                return (Guid)this.Value;
            }

            set
            {
                this.bufferType = BufferType.Guid;

                this.valueTypeBuffer.Guid = value;
            }
        }

        public int Int
        {
            get
            {
                this.ThrowIfNull();

                if (this.bufferType == BufferType.Int)
                {
                    return this.valueTypeBuffer.Int;
                }

                return (int)this.Value;
            }

            set
            {
                this.bufferType = BufferType.Int;

                this.valueTypeBuffer.Int = value;
            }
        }

        public long Long
        {
            get
            {
                this.ThrowIfNull();

                if (this.bufferType == BufferType.Long)
                {
                    return this.valueTypeBuffer.Long;
                }

                return (long)this.Value;
            }

            set
            {
                this.bufferType = BufferType.Long;

                this.valueTypeBuffer.Long = value;
            }
        }

        public short Short
        {
            get
            {
                this.ThrowIfNull();

                if (this.bufferType == BufferType.Short)
                {
                    return this.valueTypeBuffer.Short;
                }

                return (short)this.Value;
            }

            set
            {
                this.bufferType = BufferType.Short;

                this.valueTypeBuffer.Short = value;
            }
        }

        public string String
        {
            get
            {
                this.ThrowIfNull();

                if (this.bufferType == BufferType.String)
                {
                    return (string)this.referenceTypeBuffer;
                }

                return (string)this.Value;
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

                if (this.bufferType == BufferType.TimeSpan)
                {
                    return this.valueTypeBuffer.TimeSpan;
                }

                return (TimeSpan)this.Value;
            }

            set
            {
                this.bufferType = BufferType.TimeSpan;

                this.valueTypeBuffer.TimeSpan = value;
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
                    case BufferType.Bool: return this.Bool;
                    case BufferType.Byte: return this.Byte;
                    case BufferType.ByteArray: return this.ByteArray;
                    case BufferType.Char: return this.Char;
                    case BufferType.CharArray: return this.CharArray;
                    case BufferType.DateTime: return this.DateTime;
                    case BufferType.Decimal: return this.Decimal;
                    case BufferType.Double: return this.Double;
                    case BufferType.Float: return this.Float;
                    case BufferType.Guid: return this.Guid;
                    case BufferType.Int: return this.Int;
                    case BufferType.Long: return this.Long;
                    case BufferType.Short: return this.Short;
                    case BufferType.String: return this.String;
                    case BufferType.TimeSpan: return this.TimeSpan;
                }

                return null;
            }
        }

        public static void Clear(ProtoBufDataBuffer[] buffers)
        {
            for (var i = 0; i < buffers.Length; i++)
            {
                buffers[i].Clear();
            }
        }

        public static void Initialize(ProtoBufDataBuffer[] buffers)
        {
            for (var i = 0; i < buffers.Length; i++)
            {
                buffers[i] = new ProtoBufDataBuffer();
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

        [StructLayout(LayoutKind.Explicit)]
        private struct ValueTypeBuffer
        {
            [FieldOffset(0)]
            public bool Bool;
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
            public int Int;
            [FieldOffset(0)]
            public long Long;
            [FieldOffset(0)]
            public short Short;
            [FieldOffset(0)]
            public TimeSpan TimeSpan;
        }
    }
}
