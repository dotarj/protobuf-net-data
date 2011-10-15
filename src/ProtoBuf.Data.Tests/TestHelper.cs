using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using SharpTestsEx;

namespace ProtoBuf.Data.Tests
{
    public static class TestHelper
    {
        public static void AssertRowValuesEqual(DataTable expected, DataTable actual)
        {
            actual.Rows.Count.Should().Be(expected.Rows.Count);

            for (var i = 0; i < expected.Rows.Count; i++)
            {
                var originalValues = expected.Rows[i].ItemArray;
                var deserializedValues = actual.Rows[i].ItemArray;

                for (var j = 0; j < originalValues.Length; j++)
                {
                    if (originalValues[j] is byte[])
                    {
                        var sourceArray = (byte[])originalValues[j];
                        var destArray = deserializedValues[j];
                        AssertArraysEqual(sourceArray, destArray);
                    }
                    else if (originalValues[j] is char[])
                    {
                        var sourceArray = (char[])originalValues[j];
                        var destArray = deserializedValues[j];
                        AssertArraysEqual(sourceArray, destArray);
                    }
                    else
                        deserializedValues[j].Should().Be.EqualTo(originalValues[j]);
                }
            }
        }

        private static void AssertArraysEqual<T>(T[] sourceArray, object destArray)
        {
            if (sourceArray.Length == 0)
                // Zero-length arrays are deserialized as null.
                destArray.Should().Be.InstanceOf<DBNull>();
            else
                ((T[])destArray).Should().Have.SameSequenceAs(sourceArray);
        }



        public static void AssertColumnNamesEqual(DataTable expected, DataTable actual)
        {
            var expectedNames = expected.Columns
                    .OfType<DataColumn>().Select(c => c.ColumnName).ToArray();

            var actualNames = actual.Columns
                .OfType<DataColumn>().Select(c => c.ColumnName).ToArray();

            for (var i = 0; i < expectedNames.Length; i++)
                actualNames[i].Should().Be.EqualTo(expectedNames[i]);
        }

        public static void AssertColumnTypesEqual(DataTable expected, DataTable actual)
        {
            var originalTypes = expected.Columns
                    .OfType<DataColumn>().Select(c => c.DataType).ToArray();

            var deserializedTypes = actual.Columns
                .OfType<DataColumn>().Select(c => c.DataType).ToArray();

            for (var i = 0; i < originalTypes.Length; i++)
                deserializedTypes[i].Should().Be.EqualTo(originalTypes[i]);
        }
    }
}
