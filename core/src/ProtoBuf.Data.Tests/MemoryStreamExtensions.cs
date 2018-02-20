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