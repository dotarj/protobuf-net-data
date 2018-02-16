using System;
using System.IO;

namespace ProtoBuf.Data.Tests.Extensions
{
    // thanks for this mock to https://gist.github.com/hidori/2708290
    static class StreamExtensions
    {
        const int DefaultBufferSize = 4096;

        public static long CopyTo(this Stream source, Stream destination)
        {
            return CopyTo(source, destination, DefaultBufferSize, _ => { });
        }

        public static long CopyTo(this Stream source, Stream destination, int bufferSize)
        {
            return CopyTo(source, destination, bufferSize, _ => { });
        }

        public static long CopyTo(this Stream source, Stream destination, Action<long> reportProgress)
        {
            return CopyTo(source, destination, DefaultBufferSize, reportProgress);
        }

        public static long CopyTo(this Stream source, Stream destination, int bufferSize, Action<long> reportProgress)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (destination == null) throw new ArgumentNullException("destination");
            if (reportProgress == null) throw new ArgumentNullException("reportProgress");

            var buffer = new byte[bufferSize];

            var transferredBytes = 0L;

            for (var bytesRead = source.Read(buffer, 0, buffer.Length); bytesRead > 0; bytesRead = source.Read(buffer, 0, buffer.Length))
            {
                transferredBytes += bytesRead;
                reportProgress(transferredBytes);

                destination.Write(buffer, 0, bytesRead);
            }

            destination.Flush();

            return transferredBytes;
        }
    }
}