using System;

namespace ProtoBuf.Data.Internal
{
    /// <summary>
    /// Exclude a type or method from code coverage.
    /// </summary>
    /// <remarks>Using our own because the BCL one is only available in .NET
    /// 4.0 and higher.</remarks>
    [AttributeUsage(AttributeTargets.Class 
        | AttributeTargets.Constructor 
        | AttributeTargets.Method
        | AttributeTargets.Property
        | AttributeTargets.Struct)]
    internal class ExcludeFromCodeCoverageAttribute : Attribute
    {
        public ExcludeFromCodeCoverageAttribute()
        {
        }

        public ExcludeFromCodeCoverageAttribute(string reason)
        {
            
        }
    }
}