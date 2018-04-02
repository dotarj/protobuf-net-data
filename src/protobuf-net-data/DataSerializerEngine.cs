// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;

namespace ProtoBuf.Data
{
    /// <summary>
    /// Provides protocol-buffer serialization for <see cref="System.Data.IDataReader"/>s.
    /// </summary>
    public sealed class DataSerializerEngine : IDataSerializerEngine
    {
        private static readonly IProtoDataWriter Writer = new ProtoDataWriter();

        /// <summary>
        /// Serialize an <see cref="System.Data.IDataReader"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        /// <param name="reader">The <see cref="System.Data.IDataReader"/> who's contents to serialize.</param>
        public void Serialize(Stream stream, IDataReader reader)
        {
            this.Serialize(stream, reader, new ProtoDataWriterOptions());
        }

        /// <summary>
        /// Serialize a <see cref="System.Data.DataTable"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        /// <param name="dataTable">The <see cref="System.Data.DataTable"/> who's contents to serialize.</param>
        public void Serialize(Stream stream, DataTable dataTable)
        {
            this.Serialize(stream, dataTable, new ProtoDataWriterOptions());
        }

        /// <summary>
        /// Serialize a <see cref="System.Data.DataSet"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        /// <param name="dataSet">The <see cref="System.Data.DataSet"/> who's contents to serialize.</param>
        public void Serialize(Stream stream, DataSet dataSet)
        {
            this.Serialize(stream, dataSet, new ProtoDataWriterOptions());
        }

        /// <summary>
        /// Serialize an <see cref="System.Data.IDataReader"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        /// <param name="reader">The <see cref="System.Data.IDataReader"/> who's contents to serialize.</param>
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
        /// Serialize a <see cref="System.Data.DataTable"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        /// <param name="dataTable">The <see cref="System.Data.DataTable"/> who's contents to serialize.</param>
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
        /// Serialize a <see cref="System.Data.DataSet"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        /// <param name="dataSet">The <see cref="System.Data.DataSet"/> who's contents to serialize.</param>
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
        /// Deserialize a protocol-buffer binary stream back into an <see cref="System.Data.IDataReader"/>.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to read from.</param>
        public IDataReader Deserialize(Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("stream");
            }

            return new ProtoDataReader(stream);
        }

        /// <summary>
        /// Deserialize a protocol-buffer binary stream back into a <see cref="System.Data.DataTable"/>.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to read from.</param>
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
        /// Deserialize a protocol-buffer binary stream back into a <see cref="System.Data.DataSet"/>.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to read from.</param>
        /// <param name="tables">A sequence of strings, from which the <see cref="System.Data.DataSet"/> Load method retrieves table name information.</param>
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
        /// Deserialize a protocol-buffer binary stream as a <see cref="System.Data.DataSet"/>.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to read from.</param>
        /// <param name="tables">An array of strings, from which the <see cref="System.Data.DataSet"/> Load method retrieves table name information.</param>
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