// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System.Data;
using System.IO;

namespace ProtoBuf.Data
{
    /// <summary>
    /// Serializes an <see cref="System.Data.IDataReader"/> to a binary stream.
    /// </summary>
    public interface IProtoDataWriter
    {
        /// <summary>
        /// Serialize an <see cref="System.Data.IDataReader"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        /// <param name="reader">The <see cref="System.Data.IDataReader"/>who's contents to serialize.</param>
        void Serialize(Stream stream, IDataReader reader);

        /// <summary>
        /// Serialize an <see cref="System.Data.IDataReader"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        /// <param name="reader">The <see cref="System.Data.IDataReader"/> who's contents to serialize.</param>
        /// <param name="options"><see cref="ProtoDataWriterOptions"/> specifying any custom serialization options.</param>
        void Serialize(Stream stream, IDataReader reader, ProtoDataWriterOptions options);
    }
}