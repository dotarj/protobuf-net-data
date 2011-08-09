namespace ProtoBuf.Data.Internal
{
    [ProtoContract]
    public class ProtoDataColumn
    {
        [ProtoMember(1)]
        public int Ordinal;

        [ProtoMember(2)]
        public bool IsNullable;

        [ProtoMember(3, IsRequired =true)]
        public ProtoDataType ProtoDataType;

        [ProtoMember(4)]
        public string ColumnName;

        [ProtoMember(5)]
        public int OrdinalWithinType;
    }
}