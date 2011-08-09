using System;
using System.Collections.Generic;

namespace ProtoBuf.Data.Internal
{
    [ProtoContract]
    public class ProtoDataRow
    {
        // Protocol buffers doesn't encode type information* so we need to 
        // keep items as their original types. (An object[] array would not
        // be deserializable because it lacks type information).
        //
        // * actually it can, but .NET type names use too much space)

        [ProtoMember(1)]
        public List<string> StringValues;

        [ProtoMember(2)]
        public List<DateTime> DateTimeValues;

        [ProtoMember(3)]
        public List<int> Int32Values;

        [ProtoMember(4)]
        public List<long> Int64Values;

        [ProtoMember(5)]
        public List<short> Int16Values;

        [ProtoMember(6)]
        public List<bool> BoolValues;

        [ProtoMember(7)]
        public List<byte> ByteValues;

        [ProtoMember(8)]
        public List<float> FloatValues;

        [ProtoMember(9)]
        public List<double> DoubleValues;

        [ProtoMember(10)]
        public List<Guid> GuidValues;

        [ProtoMember(11)]
        public List<char> CharValues;

        [ProtoMember(12)]
        public List<decimal> DecimalValues;

        [ProtoMember(13)]
        public List<bool> NullColumns;
    }
}