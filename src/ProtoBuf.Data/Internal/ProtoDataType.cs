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
    public enum ProtoDataType
    {
        String = 1,
        DateTime = 2,
        Int = 3,
        Long = 4,
        Short = 5,
        Bool = 6,
        Byte = 7,
        Float = 8,
        Double = 9,
        Guid = 10,
        Char = 11,
        Decimal = 12,
        ByteArray = 13,
        CharArray = 14,
        DataTable = 15
    }
}