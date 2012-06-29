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

namespace ProtoBuf.Data
{
    /// <summary>
    /// Sets custom serialization options for the <see cref="ProtoDataWriter"/>.
    /// </summary>
    public class ProtoDataWriterOptions : IEquatable<ProtoDataWriterOptions>
    {
        /// <summary>
        /// In versions 2.0.4.480 and earlier, zero-length arrays were
        /// serialized as null. After that, they are serialized properly as
        /// a zero-length array. Set this flag if you need to write to the old
        /// format.
        /// </summary>
        public bool SerializeEmptyArraysAsNull { get; set; }

        public bool Equals(ProtoDataWriterOptions other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return other.SerializeEmptyArraysAsNull.Equals(SerializeEmptyArraysAsNull);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != typeof (ProtoDataWriterOptions)) return false;
            return Equals((ProtoDataWriterOptions) obj);
        }

        public override int GetHashCode()
        {
            return SerializeEmptyArraysAsNull.GetHashCode();
        }
    }
}