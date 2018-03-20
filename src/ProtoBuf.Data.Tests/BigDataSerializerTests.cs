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

using System.Data;
using System.IO;
using Xunit;

namespace ProtoBuf.Data.Tests
{
    public class BigDataSerializerTests
    {
        [Trait("Category", "Integration")]
        public class When_serializing_a_really_big_data_table_to_a_buffer_and_back
        {
            DataTable originalTable;
            DataTable deserializedTable;

            public When_serializing_a_really_big_data_table_to_a_buffer_and_back()
            {
                originalTable = TestData.DataTableFromSql("SELECT * FROM DimCustomer");
                deserializedTable = TestHelper.SerializeAndDeserialize(originalTable);
            }

            [Fact]
            public void Should_have_the_same_contents_as_the_original()
            {
                TestHelper.AssertContentsEqual(originalTable, deserializedTable);
            }
        }
    }
}