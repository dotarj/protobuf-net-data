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
using System.Data;
using System.IO;

namespace ProtoBuf.Data
{
    /// <summary>
    /// Provides protocol-buffer serialization for <see cref="System.Data.IDataReader"/>s.
    /// </summary>
    public static class DataSerializer
    {
        ///<summary>
        /// Serialize an <see cref="System.Data.IDataReader"/> to a binary stream using protocol-buffers.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        ///<param name="reader">The <see cref="System.Data.IDataReader"/>who's contents to serialize.</param>
        public static void Serialize(Stream stream, IDataReader reader)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (reader == null) throw new ArgumentNullException("reader");
            new ProtoDataWriter().Serialize(stream, reader);
        }

        ///<summary>
        /// Deserialize a protocol-buffer binary stream back into an <see cref="System.Data.IDataReader"/>.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to read from.</param>
        public static IDataReader Deserialize(Stream stream)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            return new ProtoDataReader(stream);
        }
    }
}