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

namespace ProtoBuf.Data.Internal
{
    internal static class ProtoDataTypes
    {
        static ProtoDataTypes()
        {
            var values = Enum.GetValues(typeof(ProtoDataType));
            AllTypes = new ProtoDataType[values.Length];
            for (var i = 0; i < values.Length; i++)
                AllTypes[i] = (ProtoDataType)values.GetValue(i);

            AllClrTypes = new Type[values.Length];
            for (var i = 0; i < AllTypes.Length; i++)
                AllClrTypes[i] = ConvertProtoDataType.ToClrType(AllTypes[i]);
        }

        public static ProtoDataType[] AllTypes { get; private set; }
        public static Type[] AllClrTypes { get; private set; }
    }
}