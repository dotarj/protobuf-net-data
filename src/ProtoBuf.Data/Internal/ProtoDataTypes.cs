using System;
using System.Collections.Generic;
using System.Linq;

namespace ProtoBuf.Data.Internal
{
    public static class ProtoDataTypes
    {
        public static IEnumerable<ProtoDataType> AllTypes
        {
            get { return Enum.GetValues(typeof (ProtoDataType)).OfType<ProtoDataType>(); }
        }
    }
}