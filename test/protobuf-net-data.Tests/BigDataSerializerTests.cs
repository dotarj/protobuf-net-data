// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

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
            private readonly DataTable originalTable;
            private readonly DataTable deserializedTable;

            public When_serializing_a_really_big_data_table_to_a_buffer_and_back()
            {
                this.originalTable = TestData.DataTableFromSql("SELECT * FROM DimCustomer");
                this.deserializedTable = TestHelper.SerializeAndDeserialize(this.originalTable);
            }

            [Fact(Skip = "Integration")]
            public void Should_have_the_same_contents_as_the_original()
            {
                TestHelper.AssertContentsEqual(this.originalTable, this.deserializedTable);
            }
        }
    }
}