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
using System.Data;
using System.IO;

namespace ProtoBuf.Data
{
    /// <summary>
    /// Provides protocol-buffer serialization for <see cref="System.Data.IDataReader"/>s.
    /// </summary>
    public class DataSerializerEngine : IDataSerializerEngine
    {
        static readonly IProtoDataWriter writer = new ProtoDataWriter();

        ///<summary>
        /// Serialize an <see cref="System.Data.IDataReader"/> to a binary stream using protocol-buffers.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        ///<param name="reader">The <see cref="System.Data.IDataReader"/> who's contents to serialize.</param>
        public void Serialize(Stream stream, IDataReader reader)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (reader == null) throw new ArgumentNullException("reader");

            writer.Serialize(stream, reader);
        }

        ///<summary>
        /// Serialize a <see cref="System.Data.DataTable"/> to a binary stream using protocol-buffers.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        ///<param name="dataTable">The <see cref="System.Data.DataTable"/> who's contents to serialize.</param>
        public void Serialize(Stream stream, DataTable dataTable)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (dataTable == null) throw new ArgumentNullException("dataTable");

            using (var reader = dataTable.CreateDataReader())
                Serialize(stream, reader);
        }

        ///<summary>
        /// Serialize a <see cref="System.Data.DataSet"/> to a binary stream using protocol-buffers.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        ///<param name="dataSet">The <see cref="System.Data.DataSet"/> who's contents to serialize.</param>
        public void Serialize(Stream stream, DataSet dataSet)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (dataSet == null) throw new ArgumentNullException("dataSet");

            using (var reader = dataSet.CreateDataReader())
                Serialize(stream, reader);
        }

        ///<summary>
        /// Deserialize a protocol-buffer binary stream back into an <see cref="System.Data.IDataReader"/>.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to read from.</param>
        public IDataReader Deserialize(Stream stream)
        {
            if (stream == null) throw new ArgumentNullException("stream");

            return new ProtoDataReader(stream);
        }

        ///<summary>
        /// Deserialize a protocol-buffer binary stream back into a <see cref="System.Data.DataTable"/>.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to read from.</param>
        public DataTable DeserializeDataTable(Stream stream)
        {
            if (stream == null) throw new ArgumentNullException("stream");

            var dataTable = new DataTable();
            using (var reader = Deserialize(stream))
                dataTable.Load(reader);
            return dataTable;
        }

        ///<summary>
        /// Deserialize a protocol-buffer binary stream back into a <see cref="System.Data.DataSet"/>.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to read from.</param>
        ///<param name="tables">A sequence of strings, from which the <see cref="System.Data.DataSet"/> Load method retrieves table name information.</param>
        public DataSet DeserializeDataSet(Stream stream, IEnumerable<string> tables)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (tables == null) throw new ArgumentNullException("tables");

            return DeserializeDataSet(stream, new List<string>(tables).ToArray());
        }

        ///<summary>
        /// Deserialize a protocol-buffer binary stream as a <see cref="System.Data.DataSet"/>.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to read from.</param>
        ///<param name="tables">An array of strings, from which the <see cref="System.Data.DataSet"/> Load method retrieves table name information.</param>
        public DataSet DeserializeDataSet(Stream stream, params string[] tables)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (tables == null) throw new ArgumentNullException("tables");

            var dataSet = new DataSet();
            using (var reader = Deserialize(stream))
                dataSet.Load(reader, LoadOption.OverwriteChanges, tables);

            return dataSet;
        }
    }
}