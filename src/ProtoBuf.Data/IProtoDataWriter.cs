using System.Data;
using System.IO;

namespace ProtoBuf.Data
{
    public interface IProtoDataWriter
    {
        void Serialize(Stream stream, IDataReader reader);
    }
}