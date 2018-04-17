// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;

namespace ProtoBuf.Data.Internal
{
    internal sealed class ProtoDataColumn
    {
        public ProtoDataColumn(string name, Type dataType, ProtoDataType protoBufDataType)
        {
            this.Name = name;
            this.DataType = dataType;
            this.ProtoDataType = protoBufDataType;
        }

        public string Name { get; }

        public Type DataType { get; }

        public ProtoDataType ProtoDataType { get; }
    }
}
