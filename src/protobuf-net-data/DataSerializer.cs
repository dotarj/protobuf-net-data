// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System.Collections.Generic;
using System.Data;
using System.IO;

namespace ProtoBuf.Data
{
    /// <summary>
    /// Provides protocol-buffer serialization for <see cref="System.Data.IDataReader"/>s.
    /// </summary>
    public static class DataSerializer
    {
        private static readonly IDataSerializerEngine Engine = new DataSerializerEngine();

        /// <summary>
        /// Serialize an <see cref="System.Data.IDataReader"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        /// <param name="reader">The <see cref="System.Data.IDataReader"/> who's contents to serialize.</param>
        public static void Serialize(Stream stream, IDataReader reader)
        {
            Engine.Serialize(stream, reader);
        }

        /// <summary>
        /// Serialize an <see cref="System.Data.IDataReader"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        /// <param name="reader">The <see cref="System.Data.IDataReader"/> who's contents to serialize.</param>
        /// <param name="options"><see cref="ProtoDataWriterOptions"/> specifying any custom serialization options.</param>
        public static void Serialize(Stream stream, IDataReader reader, ProtoDataWriterOptions options)
        {
            Engine.Serialize(stream, reader, options);
        }

        /// <summary>
        /// Serialize a <see cref="System.Data.DataTable"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        /// <param name="dataTable">The <see cref="System.Data.DataTable"/> who's contents to serialize.</param>
        public static void Serialize(Stream stream, DataTable dataTable)
        {
            Engine.Serialize(stream, dataTable);
        }

        /// <summary>
        /// Serialize a <see cref="System.Data.DataTable"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        /// <param name="dataTable">The <see cref="System.Data.DataTable"/> who's contents to serialize.</param>
        /// <param name="options"><see cref="ProtoDataWriterOptions"/> specifying any custom serialization options.</param>
        public static void Serialize(Stream stream, DataTable dataTable, ProtoDataWriterOptions options)
        {
            Engine.Serialize(stream, dataTable, options);
        }

        /// <summary>
        /// Serialize a <see cref="System.Data.DataSet"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        /// <param name="dataSet">The <see cref="System.Data.DataSet"/> who's contents to serialize.</param>
        public static void Serialize(Stream stream, DataSet dataSet)
        {
            Engine.Serialize(stream, dataSet);
        }

        /// <summary>
        /// Serialize a <see cref="System.Data.DataSet"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        /// <param name="dataSet">The <see cref="System.Data.DataSet"/> who's contents to serialize.</param>
        /// <param name="options"><see cref="ProtoDataWriterOptions"/> specifying any custom serialization options.</param>
        public static void Serialize(Stream stream, DataSet dataSet, ProtoDataWriterOptions options)
        {
            Engine.Serialize(stream, dataSet, options);
        }

        /// <summary>
        /// Deserialize a protocol-buffer binary stream back into an <see cref="System.Data.IDataReader"/>.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to read from.</param>
        public static IDataReader Deserialize(Stream stream)
        {
            return Engine.Deserialize(stream);
        }

        /// <summary>
        /// Deserialize a protocol-buffer binary stream back into a <see cref="System.Data.DataTable"/>.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to read from.</param>
        public static DataTable DeserializeDataTable(Stream stream)
        {
            return Engine.DeserializeDataTable(stream);
        }

        /// <summary>
        /// Deserialize a protocol-buffer binary stream back into a <see cref="System.Data.DataSet"/>.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to read from.</param>
        /// <param name="tables">A sequence of strings, from which the <see cref="System.Data.DataSet"/> Load method retrieves table name information.</param>
        public static DataSet DeserializeDataSet(Stream stream, IEnumerable<string> tables)
        {
            return Engine.DeserializeDataSet(stream, tables);
        }

        /// <summary>
        /// Deserialize a protocol-buffer binary stream as a <see cref="System.Data.DataSet"/>.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to read from.</param>
        /// <param name="tables">An array of strings, from which the <see cref="System.Data.DataSet"/> Load method retrieves table name information.</param>
        public static DataSet DeserializeDataSet(Stream stream, params string[] tables)
        {
            return Engine.DeserializeDataSet(stream, tables);
        }
    }
}