// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

namespace ProtoBuf.Data.Internal
{
    internal struct ProtoDataColumn
    {
        public ProtoDataType ProtoDataType;
        public string ColumnName;
        public int ColumnIndex;

        public override string ToString()
        {
            return string.Format("{0} ({1})", this.ColumnName, this.ProtoDataType);
        }
    }
}