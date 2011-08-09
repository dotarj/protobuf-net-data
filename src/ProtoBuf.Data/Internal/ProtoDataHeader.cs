using System.Collections.Generic;

namespace ProtoBuf.Data.Internal
{
    [ProtoContract]
    public class ProtoDataHeader
    {
        [ProtoMember(1)]
        public List<ProtoDataColumn> Columns;
    }
}