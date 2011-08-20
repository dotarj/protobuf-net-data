// Copyright 2011 Richard Dingwall - http://richarddingwall.name
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
using System.Linq;
using ProtoBuf.Data.Internal;

namespace ProtoBuf.Data
{
    public class UnsupportedColumnTypeException : Exception
    {
        public Type AttemptedType { get; private set; }

        public UnsupportedColumnTypeException(Type type)
            : base(String.Format("Cannot serialize data column of type '{0}'. Only the following column types are supported: {1}.", type, SupportedClrTypeNames))
        {
            AttemptedType = type;
        }

        static readonly string SupportedClrTypeNames;

        static UnsupportedColumnTypeException()
        {
            var types = ProtoDataTypes.AllClrTypes
                .Select(t => t.Name)
                .OrderBy(s => s);

            SupportedClrTypeNames = String.Join(", ", types);
        }
    }
}
