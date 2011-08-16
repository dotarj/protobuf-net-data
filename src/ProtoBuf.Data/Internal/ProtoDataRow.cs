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
using System.Collections.Generic;

namespace ProtoBuf.Data.Internal
{
    [ProtoContract]
    public class ProtoDataRow
    {
        // Protocol buffers doesn't encode type information* so we need to 
        // keep items as their original types. (An object[] array would not
        // be deserializable because it lacks type information).
        //
        // * actually it can, but .NET type names use too much space)

        public ProtoDataRow()
        {
            // Ensure collections aren't null (protocol buffers serializes
            // empty collections as null)
            StringValues = new List<string>();
            DateTimeValues = new List<DateTime>();
            Int32Values = new List<int>();
            Int64Values = new List<long>();
            Int16Values = new List<short>();
            BoolValues = new List<bool>();
            ByteValues = new List<byte>();
            FloatValues = new List<float>();
            DoubleValues = new List<double>();
            GuidValues = new List<Guid>();
            CharValues = new List<char>();
            DecimalValues = new List<decimal>();
            NullColumns = new List<bool>();
        }

        [ProtoMember(1)]
        public List<string> StringValues;

        [ProtoMember(2)]
        public List<DateTime> DateTimeValues;

        [ProtoMember(3)]
        public List<int> Int32Values;

        [ProtoMember(4)]
        public List<long> Int64Values;

        [ProtoMember(5)]
        public List<short> Int16Values;

        [ProtoMember(6)]
        public List<bool> BoolValues;

        [ProtoMember(7)]
        public List<byte> ByteValues;

        [ProtoMember(8)]
        public List<float> FloatValues;

        [ProtoMember(9)]
        public List<double> DoubleValues;

        [ProtoMember(10)]
        public List<Guid> GuidValues;

        [ProtoMember(11)]
        public List<char> CharValues;

        [ProtoMember(12)]
        public List<decimal> DecimalValues;

        [ProtoMember(13)]
        public List<bool> NullColumns;
    }
}