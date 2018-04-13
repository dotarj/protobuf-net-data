// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System.Collections.Generic;
using System.Data;
using System.IO;

namespace ProtoBuf.Data
{
    /// <summary>
    /// Provides protocol-buffer serialization for <see cref="IDataReader"/>s.
    /// </summary>
    public interface IDataSerializerEngine
    {
        /// <summary>
        /// Serialize an <see cref="IDataReader"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write to.</param>
        /// <param name="reader">The <see cref="IDataReader"/> who's contents to serialize.</param>
        void Serialize(Stream stream, IDataReader reader);

        /// <summary>
        /// Serialize a <see cref="DataTable"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write to.</param>
        /// <param name="dataTable">The <see cref="DataTable"/> who's contents to serialize.</param>
        void Serialize(Stream stream, DataTable dataTable);

        /// <summary>
        /// Serialize a <see cref="DataSet"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write to.</param>
        /// <param name="dataSet">The <see cref="DataSet"/> who's contents to serialize.</param>
        void Serialize(Stream stream, DataSet dataSet);

        /// <summary>
        /// Serialize an <see cref="IDataReader"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write to.</param>
        /// <param name="reader">The <see cref="IDataReader"/> who's contents to serialize.</param>
        /// <param name="options"><see cref="ProtoDataWriterOptions"/> specifying any custom serialization options.</param>
        void Serialize(Stream stream, IDataReader reader, ProtoDataWriterOptions options);

        /// <summary>
        /// Serialize a <see cref="DataTable"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write to.</param>
        /// <param name="dataTable">The <see cref="DataTable"/> who's contents to serialize.</param>
        /// <param name="options"><see cref="ProtoDataWriterOptions"/> specifying any custom serialization options.</param>
        void Serialize(Stream stream, DataTable dataTable, ProtoDataWriterOptions options);

        /// <summary>
        /// Serialize a <see cref="DataSet"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write to.</param>
        /// <param name="dataSet">The <see cref="DataSet"/> who's contents to serialize.</param>
        /// <param name="options"><see cref="ProtoDataWriterOptions"/> specifying any custom serialization options.</param>
        void Serialize(Stream stream, DataSet dataSet, ProtoDataWriterOptions options);

        /// <summary>
        /// Deserialize a protocol-buffer binary stream back into an <see cref="IDataReader"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to read from.</param>
        /// <returns>The <see cref="IDataReader"/> being deserialized.</returns>
        IDataReader Deserialize(Stream stream);

        /// <summary>
        /// Deserialize a protocol-buffer binary stream back into a <see cref="DataTable"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to read from.</param>
        /// <returns>The <see cref="DataTable"/> being deserialized.</returns>
        DataTable DeserializeDataTable(Stream stream);

        /// <summary>
        /// Deserialize a protocol-buffer binary stream back into a <see cref="DataSet"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to read from.</param>
        /// <param name="tables">A sequence of strings, from which the <see cref="DataSet"/> Load method retrieves table name information.</param>
        /// <returns>The <see cref="DataSet"/> being deserialized.</returns>
        DataSet DeserializeDataSet(Stream stream, IEnumerable<string> tables);

        /// <summary>
        /// Deserialize a protocol-buffer binary stream as a <see cref="DataSet"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to read from.</param>
        /// <param name="tables">An array of strings, from which the <see cref="DataSet"/> Load method retrieves table name information.</param>
        /// <returns>The <see cref="DataSet"/> being deserialized.</returns>
        DataSet DeserializeDataSet(Stream stream, params string[] tables);
    }
}