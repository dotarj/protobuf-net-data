// Copyright 2012 Richard Dingwall - http://richarddingwall.name
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using ProtoBuf.Data.Internal;

namespace ProtoBuf.Data
{
    /// <summary>
    /// Exception thrown when a <see cref="System.Data.IDataReader"/> with a field
    /// of an unsupported type is attempted to be serialized.
    /// </summary>
    [Serializable]
    public class UnsupportedColumnTypeException : Exception
    {
        /// <summary>
        /// The <see cref="System.Type"/> that was attempted to be serialized.
        /// </summary>
        public Type AttemptedType { get; private set; }

        /// <summary>
        /// Initializes a new instance of a <see cref="ProtoBuf.Data.UnsupportedColumnTypeException"/> type.
        /// </summary>
        /// <param name="type">The <see cref="System.Type"/> that was attempted to be serialized.</param>
        public UnsupportedColumnTypeException(Type type)
            : base(String.Format("Cannot serialize data column of type '{0}'. Only the following column types are supported: {1}.", type, SupportedClrTypeNames))
        {
            AttemptedType = type;
        }

        static readonly string SupportedClrTypeNames;

        static UnsupportedColumnTypeException()
        {
            var types = ProtoDataTypes.AllClrTypes;

            var names = new string[types.Length];

            for (var i = 0; i < types.Length; i++)
                names[i] = types[i].Name;

            Array.Sort(names);

            SupportedClrTypeNames = String.Join(", ", names);
        }
    }
}
