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

        public override string ToString()
        {
            return string.Format("{0} ({1})", ColumnName, ProtoDataType);
        }
    }
}