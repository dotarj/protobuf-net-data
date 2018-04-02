// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;

namespace ProtoBuf.Data.Internal
{
    internal static class ProtoDataTypes
    {
        static ProtoDataTypes()
        {
            Array values = Enum.GetValues(typeof(ProtoDataType));
            AllTypes = new ProtoDataType[values.Length];
            for (int i = 0; i < values.Length; i++)
            {
                AllTypes[i] = (ProtoDataType)values.GetValue(i);
            }

            AllClrTypes = new Type[values.Length];
            for (int i = 0; i < AllTypes.Length; i++)
            {
                AllClrTypes[i] = ConvertProtoDataType.ToClrType(AllTypes[i]);
            }
        }

        public static ProtoDataType[] AllTypes { get; private set; }

        public static Type[] AllClrTypes { get; private set; }
    }
}