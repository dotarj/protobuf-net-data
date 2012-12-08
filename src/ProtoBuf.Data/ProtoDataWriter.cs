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

using System;
using System.Data;
using System.IO;
using ProtoBuf.Data.Internal;

namespace ProtoBuf.Data
{
    ///<summary>
    /// Serializes an <see cref="System.Data.IDataReader"/> to a binary stream.
    ///</summary>
    public class ProtoDataWriter : IProtoDataWriter
    {
        ///<summary>
        /// Serialize an <see cref="System.Data.IDataReader"/> to a binary stream using protocol-buffers.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        ///<param name="dataSet">The <see cref="System.Data.DataSet"/>who's contents to serialize.</param>
        public void Serialize(Stream stream, DataSet dataSet)
        {
            Serialize(stream, dataSet.CreateDataReader(), new ProtoDataWriterOptions());
        }

        ///<summary>
        /// Serialize an <see cref="System.Data.IDataReader"/> to a binary stream using protocol-buffers.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        ///<param name="dataSet">The <see cref="System.Data.DataSet"/>who's contents to serialize.</param>
        public void Serialize(Stream stream, DataSet dataSet, ProtoDataWriterOptions options)
        {
            Serialize(stream, dataSet.CreateDataReader(), options);
        }

        ///<summary>
        /// Serialize an <see cref="System.Data.IDataReader"/> to a binary stream using protocol-buffers.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        ///<param name="dataTable">The <see cref="System.Data.DataTable"/>who's contents to serialize.</param>
        public void Serialize(Stream stream, DataTable dataTable)
        {
            Serialize(stream, dataTable.CreateDataReader(), new ProtoDataWriterOptions());
        }

        ///<summary>
        /// Serialize an <see cref="System.Data.IDataReader"/> to a binary stream using protocol-buffers.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        ///<param name="dataTable">The <see cref="System.Data.DataTable"/>who's contents to serialize.</param>
        public void Serialize(Stream stream, DataTable dataTable, ProtoDataWriterOptions options)
        {
            Serialize(stream, dataTable.CreateDataReader(), options);
        }

        ///<summary>
        /// Serialize an <see cref="System.Data.IDataReader"/> to a binary stream using protocol-buffers.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        ///<param name="reader">The <see cref="System.Data.IDataReader"/>who's contents to serialize.</param>
        public void Serialize(Stream stream, IDataReader reader)
        {
            Serialize(stream, reader, new ProtoDataWriterOptions());
        }

        ///<summary>
        /// Serialize an <see cref="System.Data.IDataReader"/> to a binary stream using protocol-buffers.
        ///</summary>
        ///<param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        ///<param name="reader">The <see cref="System.Data.IDataReader"/>who's contents to serialize.</param>
        ///<param name="options"><see cref="ProtoDataWriterOptions"/> specifying any custom serialization options.</param>
        public void Serialize(Stream stream, IDataReader reader, ProtoDataWriterOptions options)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (reader == null) throw new ArgumentNullException("reader");

            // Null options are permitted to be passed in.
            options = options ?? new ProtoDataWriterOptions();

            // For a (minor) performance improvement, Serialize() has been left
            // as a single long method with functions manually inlined.

            var resultIndex = 0;

            using (var writer = new ProtoWriter(stream, null, null))
            {
                do
                {
                    // This is the underlying protocol buffers structure we use:
                    //
                    // <1 StartGroup> each DataTable
                    // <SubItem>
                    //     <2 StartGroup> each DataColumn
                    //     <SubItem>
                    //         <1 String> Column Name
                    //         <2 Variant> Column ProtoDataType (enum casted to int)
                    //     </SubItem>
                    //     <3 StartGroup> each DataRow
                    //     <SubItem>
                    //         <(# Column Index) (corresponding type)> Field Value
                    //     </SubItem>
                    // </SubItem>
                    //
                    // NB if Field Value is a DataTable, the whole DataTable is 

                    // write the table
                    ProtoWriter.WriteFieldHeader(1, WireType.StartGroup, writer);
                    
                    var resultToken = ProtoWriter.StartSubItem(resultIndex, writer);

                    var columns = new ProtoDataColumnFactory().GetColumns(reader, options);

                    new HeaderWriter(writer).WriteHeader(columns);

                    var rowWriter = new RowWriter(writer, columns, options);

                    // write the rows
                    while (reader.Read())
                        rowWriter.WriteRow(reader);

                    ProtoWriter.EndSubItem(resultToken, writer);

                    resultIndex++;
                } while (reader.NextResult());
            }
        }
    }
}