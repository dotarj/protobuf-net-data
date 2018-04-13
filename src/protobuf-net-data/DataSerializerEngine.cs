// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;

namespace ProtoBuf.Data
{
    /// <summary>
    /// Provides protocol-buffer serialization for <see cref="IDataReader"/>s.
    /// </summary>
    public sealed class DataSerializerEngine : IDataSerializerEngine
    {
        private static readonly IProtoDataWriter Writer = new ProtoDataWriter();

        /// <summary>
        /// Serialize an <see cref="IDataReader"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write to.</param>
        /// <param name="reader">The <see cref="IDataReader"/> who's contents to serialize.</param>
        public void Serialize(Stream stream, IDataReader reader)
        {
            this.Serialize(stream, reader, new ProtoDataWriterOptions());
        }

        /// <summary>
        /// Serialize a <see cref="DataTable"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write to.</param>
        /// <param name="dataTable">The <see cref="DataTable"/> who's contents to serialize.</param>
        public void Serialize(Stream stream, DataTable dataTable)
        {
            this.Serialize(stream, dataTable, new ProtoDataWriterOptions());
        }

        /// <summary>
        /// Serialize a <see cref="DataSet"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write to.</param>
        /// <param name="dataSet">The <see cref="DataSet"/> who's contents to serialize.</param>
        public void Serialize(Stream stream, DataSet dataSet)
        {
            this.Serialize(stream, dataSet, new ProtoDataWriterOptions());
        }

        /// <summary>
        /// Serialize an <see cref="IDataReader"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write to.</param>
        /// <param name="reader">The <see cref="IDataReader"/> who's contents to serialize.</param>
        /// <param name="options"><see cref="ProtoDataWriterOptions"/> specifying any custom serialization options.</param>
        public void Serialize(Stream stream, IDataReader reader, ProtoDataWriterOptions options)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("stream");
            }

            if (reader == null)
            {
                throw new ArgumentNullException("reader");
            }

            Writer.Serialize(stream, reader, options);
        }

        /// <summary>
        /// Serialize a <see cref="DataTable"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write to.</param>
        /// <param name="dataTable">The <see cref="DataTable"/> who's contents to serialize.</param>
        /// <param name="options"><see cref="ProtoDataWriterOptions"/> specifying any custom serialization options.</param>
        public void Serialize(Stream stream, DataTable dataTable, ProtoDataWriterOptions options)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("stream");
            }

            if (dataTable == null)
            {
                throw new ArgumentNullException("dataTable");
            }

            using (DataTableReader reader = dataTable.CreateDataReader())
            {
                this.Serialize(stream, reader, options);
            }
        }

        /// <summary>
        /// Serialize a <see cref="DataSet"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to write to.</param>
        /// <param name="dataSet">The <see cref="DataSet"/> who's contents to serialize.</param>
        /// <param name="options"><see cref="ProtoDataWriterOptions"/> specifying any custom serialization options.</param>
        public void Serialize(Stream stream, DataSet dataSet, ProtoDataWriterOptions options)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("stream");
            }

            if (dataSet == null)
            {
                throw new ArgumentNullException("dataSet");
            }

            using (DataTableReader reader = dataSet.CreateDataReader())
            {
                this.Serialize(stream, reader, options);
            }
        }

        /// <summary>
        /// Deserialize a protocol-buffer binary stream back into an <see cref="IDataReader"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to read from.</param>
        /// <returns>The <see cref="IDataReader"/> being deserialized.</returns>
        public IDataReader Deserialize(Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("stream");
            }

            return new ProtoDataReader(stream);
        }

        /// <summary>
        /// Deserialize a protocol-buffer binary stream back into a <see cref="DataTable"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to read from.</param>
        /// <returns>The <see cref="DataTable"/> being deserialized.</returns>
        public DataTable DeserializeDataTable(Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("stream");
            }

            var dataTable = new DataTable();
            using (IDataReader reader = this.Deserialize(stream))
            {
                dataTable.Load(reader);
            }

            return dataTable;
        }

        /// <summary>
        /// Deserialize a protocol-buffer binary stream back into a <see cref="DataSet"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to read from.</param>
        /// <param name="tables">A sequence of strings, from which the <see cref="DataSet"/> Load method retrieves table name information.</param>
        /// <returns>The <see cref="DataSet"/> being deserialized.</returns>
        public DataSet DeserializeDataSet(Stream stream, IEnumerable<string> tables)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("stream");
            }

            if (tables == null)
            {
                throw new ArgumentNullException("tables");
            }

            return this.DeserializeDataSet(stream, new List<string>(tables).ToArray());
        }

        /// <summary>
        /// Deserialize a protocol-buffer binary stream as a <see cref="DataSet"/>.
        /// </summary>
        /// <param name="stream">The <see cref="Stream"/> to read from.</param>
        /// <param name="tables">An array of strings, from which the <see cref="DataSet"/> Load method retrieves table name information.</param>
        /// <returns>The <see cref="DataSet"/> being deserialized.</returns>
        public DataSet DeserializeDataSet(Stream stream, params string[] tables)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("stream");
            }

            if (tables == null)
            {
                throw new ArgumentNullException("tables");
            }

            var dataSet = new DataSet();
            using (IDataReader reader = this.Deserialize(stream))
            {
                dataSet.Load(reader, LoadOption.OverwriteChanges, tables);
            }

            return dataSet;
        }
    }
}