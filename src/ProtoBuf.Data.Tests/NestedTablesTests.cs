// Copyright 2011 Richard Dingwall - http://richarddingwall.name
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
using NUnit.Framework;

namespace ProtoBuf.Data.Tests
{
    public class NestedTablesTests
    {
        [TestFixture]
        public class When_serializing_a_datareader_with_nested_tables_sets
        {
            private DataTable table;
            private DataTable deserializedTable;

            [TestFixtureSetUp]
            public void TestFixtureSetUp()
            {
                var innerA = TestData.FromMatrix(new[]
                                        {
                                            new object[] {"A", "B"},
                                            new object[] {1, 2},
                                            new object[] {3, 4}
                                        });

                var innerB = TestData.FromMatrix(new[]
                                        {
                                            new object[] {"C", "D"},
                                            new object[] {5, 6},
                                            new object[] {7, 8}
                                        });

                table = TestData.FromMatrix(new[]
                                                     {
                                                         new object[] {"E", "F"},
                                                         new object[] {"A", innerA},
                                                         new object[] {"B", innerB}
                                                     });

                deserializedTable = TestHelper.SerializeAndDeserialize(table);
            }

            [Test]
            public void All_tables_should_have_the_same_contents()
            {
                TestHelper.AssertContentsEqual(table, deserializedTable);
            }
        }
    }
}