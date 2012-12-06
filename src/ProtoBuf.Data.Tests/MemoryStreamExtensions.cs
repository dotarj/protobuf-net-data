using System.IO;

namespace ProtoBuf.Data.Tests
{
    public static class MemoryStreamExtensions
    {
        // GetBuffer() can return junk bytes at the end, because internally it
        // always allocates more than it needs. This one doesn't.
        public static byte[] GetTrimmedBuffer(this MemoryStream stream)
        {
            stream.Seek(0, SeekOrigin.Begin);
            var trimmedBuffer = new byte[stream.Length];
            stream.Read(trimmedBuffer, 0, (int)stream.Length);
            return trimmedBuffer;
        }
    }
}