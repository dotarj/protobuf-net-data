// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System.Collections.Generic;
using System.Data;
using System.IO;

namespace ProtoBuf.Data
{
    /// <summary>
    /// Provides protocol-buffer serialization for <see cref="IDataReader"/>s.
    /// </summary>
    public static class DataSerializer
    {
        private static readonly IDataSerializerEngine Engine = new DataSerializerEngine();

        /// <summary>
        /// Serialize an <see cref="IDataReader"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write to.</param>
        /// <param name="reader">The <see cref="IDataReader"/> who's contents to serialize.</param>
        public static void Serialize(Stream stream, IDataReader reader)
        {
            Engine.Serialize(stream, reader);
        }

        /// <summary>
        /// Serialize an <see cref="IDataReader"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write to.</param>
        /// <param name="reader">The <see cref="IDataReader"/> who's contents to serialize.</param>
        /// <param name="options"><see cref="ProtoDataWriterOptions"/> specifying any custom serialization options.</param>
        public static void Serialize(Stream stream, IDataReader reader, ProtoDataWriterOptions options)
        {
            Engine.Serialize(stream, reader, options);
        }

        /// <summary>
        /// Serialize a <see cref="DataTable"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write to.</param>
        /// <param name="dataTable">The <see cref="DataTable"/> who's contents to serialize.</param>
        public static void Serialize(Stream stream, DataTable dataTable)
        {
            Engine.Serialize(stream, dataTable);
        }

        /// <summary>
        /// Serialize a <see cref="DataTable"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write to.</param>
        /// <param name="dataTable">The <see cref="DataTable"/> who's contents to serialize.</param>
        /// <param name="options"><see cref="ProtoDataWriterOptions"/> specifying any custom serialization options.</param>
        public static void Serialize(Stream stream, DataTable dataTable, ProtoDataWriterOptions options)
        {
            Engine.Serialize(stream, dataTable, options);
        }

        /// <summary>
        /// Serialize a <see cref="DataSet"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write to.</param>
        /// <param name="dataSet">The <see cref="DataSet"/> who's contents to serialize.</param>
        public static void Serialize(Stream stream, DataSet dataSet)
        {
            Engine.Serialize(stream, dataSet);
        }

        /// <summary>
        /// Serialize a <see cref="DataSet"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write to.</param>
        /// <param name="dataSet">The <see cref="DataSet"/> who's contents to serialize.</param>
        /// <param name="options"><see cref="ProtoDataWriterOptions"/> specifying any custom serialization options.</param>
        public static void Serialize(Stream stream, DataSet dataSet, ProtoDataWriterOptions options)
        {
            Engine.Serialize(stream, dataSet, options);
        }

        /// <summary>
        /// Deserialize a protocol-buffer binary stream back into an <see cref="IDataReader"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to read from.</param>
        /// <returns>The <see cref="IDataReader"/> being deserialized.</returns>
        public static IDataReader Deserialize(Stream stream)
        {
            return Engine.Deserialize(stream);
        }

        /// <summary>
        /// Deserialize a protocol-buffer binary stream back into a <see cref="DataTable"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to read from.</param>
        /// <returns>The <see cref="DataTable"/> being deserialized.</returns>
        public static DataTable DeserializeDataTable(Stream stream)
        {
            return Engine.DeserializeDataTable(stream);
        }

        /// <summary>
        /// Deserialize a protocol-buffer binary stream back into a <see cref="DataSet"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to read from.</param>
        /// <param name="tables">A sequence of strings, from which the <see cref="DataSet"/> Load method retrieves table name information.</param>
        /// <returns>The <see cref="DataSet"/> being deserialized.</returns>
        public static DataSet DeserializeDataSet(Stream stream, IEnumerable<string> tables)
        {
            return Engine.DeserializeDataSet(stream, tables);
        }

        /// <summary>
        /// Deserialize a protocol-buffer binary stream as a <see cref="DataSet"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to read from.</param>
        /// <param name="tables">An array of strings, from which the <see cref="DataSet"/> Load method retrieves table name information.</param>
        /// <returns>The <see cref="DataSet"/> being deserialized.</returns>
        public static DataSet DeserializeDataSet(Stream stream, params string[] tables)
        {
            return Engine.DeserializeDataSet(stream, tables);
        }
    }
}