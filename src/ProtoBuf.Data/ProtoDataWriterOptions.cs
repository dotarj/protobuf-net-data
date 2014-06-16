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

namespace ProtoBuf.Data
{
    using System;

    /// <summary>
    /// Sets custom serialization options for the <see cref="ProtoDataWriter"/>.
    /// </summary>
    public sealed class ProtoDataWriterOptions : IEquatable<ProtoDataWriterOptions>
    {
        /// <summary>
        /// Gets or sets a value indicating whether zero-length arrays were
        /// serialized as null. In versions 2.0.4.480 and earlier, zero-length
        /// arrays were serialized as null. After that, they are serialized
        /// properly as a zero-length array. Set this flag if you need to write
        /// to the old format. Default is false.
        /// </summary>
        public bool SerializeEmptyArraysAsNull { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether computed columns are
        /// ignored by default (columns who's values are determined by an
        /// Expression rather than a stored value). Set to true to include
        /// computed columns in serialization.
        /// </summary>
        public bool IncludeComputedColumns { get; set; }

        /// <summary>
        /// Indicates whether the current object is equal to another object of the same type.
        /// </summary>
        /// <returns>
        /// true if the current object is equal to the <paramref name="other"/> parameter; otherwise, false.
        /// </returns>
        /// <param name="other">An object to compare with this object.</param>
        public bool Equals(ProtoDataWriterOptions other)
        {
            if (ReferenceEquals(null, other))
            {
                return false;
            }

            if (ReferenceEquals(this, other))
            {
                return true;
            }

            return other.SerializeEmptyArraysAsNull.Equals(SerializeEmptyArraysAsNull) && other.IncludeComputedColumns.Equals(IncludeComputedColumns);
        }

        /// <summary>
        /// Determines whether the specified <see cref="T:System.Object"/> is equal to the current <see cref="T:System.Object"/>.
        /// </summary>
        /// <returns>
        /// true if the specified <see cref="T:System.Object"/> is equal to the current <see cref="T:System.Object"/>; otherwise, false.
        /// </returns>
        /// <param name="obj">The <see cref="T:System.Object"/> to compare with the current <see cref="T:System.Object"/>. 
        /// </param><exception cref="T:System.NullReferenceException">The <paramref name="obj"/> parameter is null.
        /// </exception><filterpriority>2</filterpriority>
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj.GetType() != typeof(ProtoDataWriterOptions))
            {
                return false;
            }

            return Equals((ProtoDataWriterOptions)obj);
        }

        /// <summary>
        /// Serves as a hash function for a particular type. 
        /// </summary>
        /// <returns>
        /// A hash code for the current <see cref="T:System.Object"/>.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override int GetHashCode()
        {
            unchecked
            {
                return (SerializeEmptyArraysAsNull.GetHashCode() * 397) ^ IncludeComputedColumns.GetHashCode();
            }
        }
    }
}