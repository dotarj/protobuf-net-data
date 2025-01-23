// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

namespace ProtoBuf.Data.Internal
{
    internal enum ProtoDataType
    {
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
        TimeSpan = 15,
        DateTimeOffset = 16
    }
}