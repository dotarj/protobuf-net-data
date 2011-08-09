using System;

namespace ProtoBuf.Data
{
    public class UnsupportedColumnTypeException : Exception
    {
        public UnsupportedColumnTypeException(Type type) : base(String.Format("Cannot serialize data column of type '{0}'. Only primitive types are supported.", type))
        {
            
        }
    }
}
