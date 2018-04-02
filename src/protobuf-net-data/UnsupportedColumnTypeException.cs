// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using ProtoBuf.Data.Internal;

namespace ProtoBuf.Data
{
    /// <summary>
    /// Exception thrown when a <see cref="System.Data.IDataReader"/> with a field
    /// of an unsupported type is attempted to be serialized.
    /// </summary>
    [Serializable]
    public sealed class UnsupportedColumnTypeException : Exception
    {
        private static readonly string SupportedClrTypeNames;

        static UnsupportedColumnTypeException()
        {
            Type[] types = ProtoDataTypes.AllClrTypes;

            var names = new string[types.Length];

            for (int i = 0; i < types.Length; i++)
            {
                names[i] = types[i].Name;
            }

            Array.Sort(names);

            SupportedClrTypeNames = string.Join(", ", names);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="UnsupportedColumnTypeException"/> class.
        /// </summary>
        /// <param name="type">
        /// The <see cref="System.Type"/> that was attempted to be serialized.
        /// </param>
        public UnsupportedColumnTypeException(Type type)
            : base(string.Format("Cannot serialize data column of type '{0}'. Only the following column types are supported: {1}.", type, SupportedClrTypeNames))
        {
            this.AttemptedType = type;
        }

        /// <summary>
        /// Gets the <see cref="System.Type"/> that was attempted to be serialized.
        /// </summary>
        public Type AttemptedType { get; private set; }
    }
}
