using System.Collections.Generic;
using System.Diagnostics;

namespace ProtoBuf.Data.Internal
{
    [ProtoContract]
    [DebuggerDisplay("[{Columns.Count} columns]")]
    public class ProtoDataHeader
    {
        public ProtoDataHeader()
        {
            Columns = new List<ProtoDataColumn>();
        }

        [ProtoMember(1)]
        public List<ProtoDataColumn> Columns;
    }
}