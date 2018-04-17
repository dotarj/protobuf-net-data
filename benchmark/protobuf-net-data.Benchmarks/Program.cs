// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using BenchmarkDotNet.Running;

namespace ProtoBuf.Data.Benchmarks
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            var switcher = new BenchmarkSwitcher(new[]
            {
                typeof(SerializeBenchmark),
                typeof(DeserializeBenchmark)
            });

            switcher.Run(args);
        }
    }
}
