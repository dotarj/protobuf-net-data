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
        public static void AssertValuesEqual(DataTable expected, DataTable actual)
        {
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
                    else
                        deserializedValues[i].Should().Be.EqualTo(originalValues[i]);
                }
            }
        }

        private static void AssertArraysEqual(byte[] sourceArray, object destArray)
        {
            if (sourceArray.Length == 0)
                // Zero-length arrays are deserialized as null.
                destArray.Should().Be.InstanceOf<DBNull>();
            else
                ((byte[])destArray).Should().Have.SameSequenceAs(sourceArray);
        }
    }
}
