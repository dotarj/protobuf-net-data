// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;

namespace ProtoBuf.Data.Internal
{
    internal static class ConvertProtoDataType
    {
        private static readonly IDictionary<Type, ProtoDataType> Mapping
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
                      { typeof(decimal), ProtoDataType.Decimal },
                      { typeof(byte[]), ProtoDataType.ByteArray },
                      { typeof(char[]), ProtoDataType.CharArray },
                      { typeof(TimeSpan), ProtoDataType.TimeSpan },
                  };

        public static ProtoDataType FromClrType(Type type)
        {
            ProtoDataType value;
            if (Mapping.TryGetValue(type, out value))
            {
                return value;
            }

            throw new UnsupportedColumnTypeException(type);
        }

        public static Type ToClrType(ProtoDataType type)
        {
            foreach (var pair in Mapping)
            {
                if (pair.Value.Equals(type))
                {
                    return pair.Key;
                }
            }

            throw new InvalidOperationException("Unknown ProtoDataType.");
        }
    }
}