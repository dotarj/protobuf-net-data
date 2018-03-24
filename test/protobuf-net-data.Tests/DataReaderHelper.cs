// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System.Data;
using System.IO;

namespace ProtoBuf.Data.Tests
{
    internal static class DataReaderHelper
    {
        public static ProtoDataReader CreateDataReader<TDataType>(TDataType value)
        {
            var dataTable = new DataTable();

            dataTable.Columns.Add(typeof(TDataType).Name, typeof(TDataType));

            dataTable.Rows.Add(value);

            return ToProtoDataReader(dataTable.CreateDataReader());
        }

        public static ProtoDataReader ToProtoDataReader(IDataReader dataReader)
        {
            var memoryStream = new MemoryStream();

            DataSerializer.Serialize(memoryStream, dataReader);

            memoryStream.Position = 0;

            return (ProtoDataReader)DataSerializer.Deserialize(memoryStream);
        }
    }
}
