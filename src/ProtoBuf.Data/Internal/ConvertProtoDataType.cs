using System;
using System.Collections.Generic;
using System.Linq;

namespace ProtoBuf.Data.Internal
{
    public static class ConvertProtoDataType
    {
        static readonly IDictionary<Type, ProtoDataType> mapping
            = new Dictionary<Type, ProtoDataType>
                  {
                      { typeof(bool), ProtoDataType.Bool },
                      { typeof(byte), ProtoDataType.Byte },
                      { typeof(DateTime), ProtoDataType.DateTime },
                      { typeof(double), ProtoDataType.Double },
                      { typeof(float), ProtoDataType.Float },
                      { typeof(Guid), ProtoDataType.Guid },
                      { typeof(int), ProtoDataType.Int },
                      { typeof(long), ProtoDataType.Long },
                      { typeof(short), ProtoDataType.Short },
                      { typeof(string), ProtoDataType.String },
                      { typeof(char), ProtoDataType.Char },
                      { typeof(decimal), ProtoDataType.Decimal }
                  };

        public static ProtoDataType FromClrType(Type type)
        {
            ProtoDataType value;
            if (mapping.TryGetValue(type, out value))
                return value;

            throw new UnsupportedColumnTypeException(type);
        }

        public static Type ToClrType(ProtoDataType type)
        {
            return mapping.Single(p => p.Value.Equals(type)).Key;
        }
    }
}