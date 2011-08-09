using System;
using System.Data;
using System.IO;

namespace ProtoBuf.Data
{
    public static class DataSerializer
    {
        public static void Serialize(Stream stream, IDataReader reader)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (reader == null) throw new ArgumentNullException("reader");
            new ProtoDataWriter().Serialize(stream, reader);
        }

        public static IDataReader Deserialize(Stream stream)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            return new ProtoDataReader(stream);
        }
    }
}