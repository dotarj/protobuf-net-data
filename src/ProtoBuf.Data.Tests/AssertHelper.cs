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

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using NUnit.Framework;

namespace ProtoBuf.Data.Tests
{
    public static class AssertHelper
    {
        public static void AssertContentsEqual(DataSet expected, DataSet actual)
        {
            Assert.That(actual.Tables.Count, Is.EqualTo(expected.Tables.Count), 
                "DataSets had a different number of tables.");

            for (var i = 0; i < expected.Tables.Count; i++)
                AssertContentsEqual(expected.Tables[i], actual.Tables[i]);
        }

        public static void AssertContentsEqual(DataTable expected, DataTable actual)
        {
            if (expected == null) throw new ArgumentNullException("expected");

            Assert.That(actual, Is.Not.Null);

            Assert.That(actual.Columns.Count, Is.EqualTo(expected.Columns.Count),
                        "Table {0} had a different number of columns.", GetTableNumber(actual));

            AssertColumnNamesEqual(expected, actual);
            AssertColumnTypesEqual(expected, actual);
            AssertRowValuesEqual(expected, actual);
        }

        static string GetTableNumber(DataTable table)
        {
            if (table.DataSet == null)
                return "";

            return String.Format("{0}", table.DataSet.Tables.IndexOf(table) + 1);
        }

        static void AssertRowValuesEqual(DataTable expected, DataTable actual)
        {
            Assert.That(actual.Rows.Count, Is.EqualTo(expected.Rows.Count), 
                "Table {0} had a different number of rows.", GetTableNumber(actual));

            for (var i = 0; i < expected.Rows.Count; i++)
            {
                var expectedItemArray = expected.Rows[i].ItemArray;
                var actualItemArray = actual.Rows[i].ItemArray;

                for (var j = 0; j < expectedItemArray.Length; j++)
                {
                    if (expectedItemArray[j] is byte[])
                    {
                        var sourceArray = (byte[])expectedItemArray[j];
                        var destArray = actualItemArray[j];
                        AssertArraysEqual(sourceArray, destArray);
                    }
                    else if (expectedItemArray[j] is char[])
                    {
                        var sourceArray = (char[])expectedItemArray[j];
                        var destArray = actualItemArray[j];
                        AssertArraysEqual(sourceArray, destArray);
                    }
                    else
                        Assert.That(actualItemArray[j], Is.EqualTo(expectedItemArray[j]), 
                            "Table {0} values at row {1}, column {2} don't match.", GetTableNumber(actual), i, j);
                }
            }
        }

        static void AssertArraysEqual<T>(ICollection<T> sourceArray, object destArray)
        {
            if (sourceArray.Count == 0)
                Assert.That(destArray, Is.InstanceOf<DBNull>(), "Zero-length arrays should be deserialized as null.");
            else
                Assert.That(destArray, Is.EquivalentTo(sourceArray), "Array fields contents didn't match.");
        }

        static void AssertColumnNamesEqual(DataTable expected, DataTable actual)
        {
            var expectedNames = GetColumnNames(expected);
            var actualNames = GetColumnNames(actual);

            Assert.That(actualNames, Is.EquivalentTo(expectedNames), 
                "Table {0} column names didn't match.", GetTableNumber(actual));
        }

        static string[] GetColumnNames(DataTable dataTable)
        {
            return dataTable.Columns.OfType<DataColumn>().Select(c => c.ColumnName).ToArray();
        }

        static Type[] GetColumnTypes(DataTable dataTable)
        {
            return dataTable.Columns.OfType<DataColumn>().Select(c => c.DataType).ToArray();
        }

        static void AssertColumnTypesEqual(DataTable expected, DataTable actual)
        {
            var expectedTypes = GetColumnTypes(expected);
            var actualTypes = GetColumnTypes(actual);

            Assert.That(actualTypes, Is.EquivalentTo(expectedTypes), 
                "Table {0} column types didn't match.", GetTableNumber(actual));
        }
    }
}
