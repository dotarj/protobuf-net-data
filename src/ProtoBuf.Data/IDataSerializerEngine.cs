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

using System.Collections.Generic;
using System.Data;
using System.IO;

namespace ProtoBuf.Data
{
    /// <summary>
    /// Provides protocol-buffer serialization for <see cref="System.Data.IDataReader"/>s.
    /// </summary>
    public interface IDataSerializerEngine
    {
        ///<summary>
        /// Serialize an <see cref="System.Data.IDataReader"/> to a binary stream using protocol-buffers.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        ///<param name="reader">The <see cref="System.Data.IDataReader"/> who's contents to serialize.</param>
        void Serialize(Stream stream, IDataReader reader);

        ///<summary>
        /// Serialize a <see cref="System.Data.DataTable"/> to a binary stream using protocol-buffers.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        ///<param name="dataTable">The <see cref="System.Data.DataTable"/> who's contents to serialize.</param>
        void Serialize(Stream stream, DataTable dataTable);

        ///<summary>
        /// Serialize a <see cref="System.Data.DataSet"/> to a binary stream using protocol-buffers.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        ///<param name="dataSet">The <see cref="System.Data.DataSet"/> who's contents to serialize.</param>
        void Serialize(Stream stream, DataSet dataSet);

        ///<summary>
        /// Serialize an <see cref="System.Data.IDataReader"/> to a binary stream using protocol-buffers.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        ///<param name="reader">The <see cref="System.Data.IDataReader"/> who's contents to serialize.</param>
        ///<param name="options"><see cref="ProtoDataWriterOptions"/> specifying any custom serialization options.</param>
        void Serialize(Stream stream, IDataReader reader, ProtoDataWriterOptions options);

        ///<summary>
        /// Serialize a <see cref="System.Data.DataTable"/> to a binary stream using protocol-buffers.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        ///<param name="dataTable">The <see cref="System.Data.DataTable"/> who's contents to serialize.</param>
        ///<param name="options"><see cref="ProtoDataWriterOptions"/> specifying any custom serialization options.</param>
        void Serialize(Stream stream, DataTable dataTable, ProtoDataWriterOptions options);

        ///<summary>
        /// Serialize a <see cref="System.Data.DataSet"/> to a binary stream using protocol-buffers.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        ///<param name="dataSet">The <see cref="System.Data.DataSet"/> who's contents to serialize.</param>
        ///<param name="options"><see cref="ProtoDataWriterOptions"/> specifying any custom serialization options.</param>
        void Serialize(Stream stream, DataSet dataSet, ProtoDataWriterOptions options);

        ///<summary>
        /// Deserialize a protocol-buffer binary stream back into an <see cref="System.Data.IDataReader"/>.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to read from.</param>
        IDataReader Deserialize(Stream stream);

        ///<summary>
        /// Deserialize a protocol-buffer binary stream back into a <see cref="System.Data.DataTable"/>.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to read from.</param>
        DataTable DeserializeDataTable(Stream stream);

        ///<summary>
        /// Deserialize a protocol-buffer binary stream back into a <see cref="System.Data.DataSet"/>.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to read from.</param>
        ///<param name="tables">A sequence of strings, from which the <see cref="System.Data.DataSet"/> Load method retrieves table name information.</param>
        DataSet DeserializeDataSet(Stream stream, IEnumerable<string> tables);

        ///<summary>
        /// Deserialize a protocol-buffer binary stream as a <see cref="System.Data.DataSet"/>.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to read from.</param>
        ///<param name="tables">An array of strings, from which the <see cref="System.Data.DataSet"/> Load method retrieves table name information.</param>
        DataSet DeserializeDataSet(Stream stream, params string[] tables);
    }
}