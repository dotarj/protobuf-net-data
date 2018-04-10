// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using BenchmarkDotNet.Running;

namespace ProtoBuf.Data.Benchmarks
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            BenchmarkRunner.Run<SerializeBenchmark>();
            BenchmarkRunner.Run<DeserializeBenchmark>();

            Console.Read();
        }
    }
}
