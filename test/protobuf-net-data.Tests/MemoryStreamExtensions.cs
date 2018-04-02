// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System.IO;

namespace ProtoBuf.Data.Tests
{
    public static class MemoryStreamExtensions
    {
        // GetBuffer() can return a bunch of NUL bytes at the end, because
        // internally it allocated more than it eventually needed. This one
        // doesn't.
        public static byte[] GetTrimmedBuffer(this MemoryStream stream)
        {
            stream.Seek(0, SeekOrigin.Begin);
            var trimmedBuffer = new byte[stream.Length];
            stream.Read(trimmedBuffer, 0, (int)stream.Length);
            return trimmedBuffer;
        }
    }
}