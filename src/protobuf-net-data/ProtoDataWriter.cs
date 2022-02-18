// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

#pragma warning disable CS0618
using System.Data;
using System.IO;
using ProtoBuf.Data.Internal;

namespace ProtoBuf.Data
{
    /// <summary>
    /// Serializes an <see cref="System.Data.IDataReader"/> to a binary stream.
    /// </summary>
    public class ProtoDataWriter : IProtoDataWriter
    {
        /// <summary>
        /// Serialize an <see cref="System.Data.IDataReader"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        /// <param name="dataSet">The <see cref="System.Data.DataSet"/>who's contents to serialize.</param>
        public void Serialize(Stream stream, DataSet dataSet)
        {
            this.Serialize(stream, dataSet.CreateDataReader(), new ProtoDataWriterOptions());
        }

        /// <summary>
        /// Serialize an <see cref="System.Data.IDataReader"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        /// <param name="dataSet">The <see cref="System.Data.DataSet"/>who's contents to serialize.</param>
        /// <param name="options">Writer options.</param>
        public void Serialize(Stream stream, DataSet dataSet, ProtoDataWriterOptions options)
        {
            this.Serialize(stream, dataSet.CreateDataReader(), options);
        }

        /// <summary>
        /// Serialize an <see cref="System.Data.IDataReader"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        /// <param name="dataTable">The <see cref="System.Data.DataTable"/>who's contents to serialize.</param>
        public void Serialize(Stream stream, DataTable dataTable)
        {
            this.Serialize(stream, dataTable.CreateDataReader(), new ProtoDataWriterOptions());
        }

        /// <summary>
        /// Serialize an <see cref="System.Data.IDataReader"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        /// <param name="dataTable">The <see cref="System.Data.DataTable"/>who's contents to serialize.</param>
        /// <param name="options">Writer options.</param>
        public void Serialize(Stream stream, DataTable dataTable, ProtoDataWriterOptions options)
        {
            this.Serialize(stream, dataTable.CreateDataReader(), options);
        }

        /// <summary>
        /// Serialize an <see cref="System.Data.IDataReader"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        /// <param name="reader">The <see cref="System.Data.IDataReader"/>who's contents to serialize.</param>
        public void Serialize(Stream stream, IDataReader reader)
        {
            this.Serialize(stream, reader, new ProtoDataWriterOptions());
        }

        /// <summary>
        /// Serialize an <see cref="System.Data.IDataReader"/> to a binary stream using protocol-buffers.
        /// </summary>
        /// <param name="stream">The <see cref="System.IO.Stream"/> to write to.</param>
        /// <param name="reader">The <see cref="System.Data.IDataReader"/>who's contents to serialize.</param>
        /// <param name="options"><see cref="ProtoDataWriterOptions"/> specifying any custom serialization options.</param>
        public void Serialize(Stream stream, IDataReader reader, ProtoDataWriterOptions options)
        {
            Throw.IfNull(stream, nameof(stream));
            Throw.IfNull(reader, nameof(reader));

            options = options ?? new ProtoDataWriterOptions();

            var resultIndex = 0;

            using (var writer = ProtoWriter.Create(stream, null, null))
            {
                var context = new ProtoWriterContext(writer, options);

                do
                {
                    ProtoWriter.WriteFieldHeader(1, WireType.StartGroup, writer);

                    context.StartSubItem(resultIndex);

                    context.Columns = ProtoDataColumnFactory.GetColumns(reader, options);

                    ColumnsWriter.WriteColumns(context);

                    var recordIndex = 0;

                    while (reader.Read())
                    {
                        RecordWriter.WriteRecord(context, recordIndex, reader);

                        recordIndex++;
                    }

                    context.EndSubItem();

                    resultIndex++;
                }
                while (reader.NextResult());

                // necessary since protobuf-net v3
                writer.Close();
            }
        }
    }
}
#pragma warning restore CS0618
