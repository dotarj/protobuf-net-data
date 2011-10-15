using System;
using System.Data;
using System.Linq;

namespace ProtoBuf.Data.Tests
{
    public static class TestData
    {
        public static DataTable SmallDataTable()
        {
            var table = new DataTable();
            table.Columns.Add("Birthday", typeof(DateTime));
            table.Columns.Add("Age", typeof(int));
            table.Columns.Add("Name", typeof(string));
            table.Columns.Add("ID", typeof(Guid));
            table.Columns.Add("LastName", typeof(string));
            table.Columns.Add("BlobData", typeof(byte[]));
            table.Columns.Add("ClobData", typeof(char[]));
            table.Rows.Add(new DateTime(2011, 04, 05, 12, 16, 41, 300), 42, "Foo", Guid.Parse("6891816b-a4b9-4749-a9f5-9f6deb377a65"), "sdfsdf", new byte[] { 1, 2, 3, 4 }, new[] { 'a' });
            table.Rows.Add(new DateTime(1920, 04, 03, 12, 48, 31, 210), null, "Bar", Guid.Parse("28545f31-ca0c-40c1-bae0-9b79ca84091b"), "o2389uf", new byte[0], new[] { 'a', 'b', 'c' });
            table.Rows.Add(null, null, null, null, null, null, null);
            table.Rows.Add(new DateTime(2008, 01, 11, 11, 4, 1, 491), null, "Foo", Guid.Empty, "", null, new char[0]);
            return table;
        }

        public static DataTable DifferentSmallDataTable()
        {
            var table = new DataTable();
            table.Columns.Add("Dog", typeof(int));
            table.Columns.Add("Banana", typeof(string));
            table.Columns.Add("Cat", typeof(DateTime));
            table.Rows.Add(42, "Foo", new DateTime(2011, 04, 05, 12, 16, 41, 300));
            table.Rows.Add(null, "Bar", new DateTime(1920, 04, 03, 12, 48, 31, 210));
            table.Rows.Add(null, null, null, null, null, null, null);
            return table;
        }

        public static DataTable FromMatrix(object[][] matrix)
        {
            var table = new DataTable();

            // First row has column names. Guess the type of each column from data in the 2nd row.
            var columnNames = matrix[0];
            var firstRow = matrix[1];

            for (var i = 0; i < columnNames.Count(); i++)
                table.Columns.Add((string)columnNames[i], firstRow[i].GetType());

            for (var i = 1; i < matrix.Length; i++)
                table.Rows.Add(matrix[i]);

            return table;
        }
    }
}
