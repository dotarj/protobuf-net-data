// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace ProtoBuf.Data.Tests
{
    public static class TestData
    {
        private static readonly Random Random = new Random();

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
            table.Columns.Add("SomeFloat", typeof(float));
            table.Columns.Add("SomeDouble", typeof(double));
            table.Columns.Add("SomeChar", typeof(char));
            table.Columns.Add("SomeTimeSpan", typeof(TimeSpan));
            table.Rows.Add(new DateTime(2011, 04, 05, 12, 16, 41, 300), 42, "Foo", Guid.Parse("6891816b-a4b9-4749-a9f5-9f6deb377a65"), "sdfsdf", new byte[] { 1, 2, 3, 4 }, new[] { 'a' }, 0.1, 0.2, null, TimeSpan.Zero);
            table.Rows.Add(new DateTime(1920, 04, 03, 12, 48, 31, 210), null, "Bar", Guid.Parse("28545f31-ca0c-40c1-bae0-9b79ca84091b"), "o2389uf", new byte[0], new[] { 'a', 'b', 'c' }, 0.3, 0.4, 'c', TimeSpan.Zero);
            table.Rows.Add(null, null, null, null, null, null, null, null, null, 'h', TimeSpan.FromMinutes(2));
            table.Rows.Add(new DateTime(2008, 01, 11, 11, 4, 1, 491), null, "Foo", Guid.Empty, string.Empty, null, new char[0], -0.3, -0.4, 'a', TimeSpan.FromDays(1));

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
            table.Rows.Add(null, null, null);
            return table;
        }

        public static DataTable FromMatrix(object[][] matrix)
        {
            var table = new DataTable();

            // First row has column names. Guess the type of each column from data in the 2nd row.
            var columnNames = matrix[0];
            var firstRow = matrix[1];

            for (var i = 0; i < columnNames.Count(); i++)
            {
                table.Columns.Add((string)columnNames[i], firstRow[i].GetType());
            }

            for (var i = 1; i < matrix.Length; i++)
            {
                table.Rows.Add(matrix[i]);
            }

            return table;
        }

        public static IDataReader DataReaderFromSql(string sql)
        {
            if (string.IsNullOrWhiteSpace(sql))
            {
                throw new ArgumentException("String was null or empty.", "sql");
            }

            var connectionString = new SqlConnectionStringBuilder
            {
                DataSource = @".\SQLEXPRESS",
                InitialCatalog = "AdventureWorksDW2008R2",
                IntegratedSecurity = true
            };

            var connection = new SqlConnection(connectionString.ConnectionString);
            try
            {
                connection.Open();
            }
            catch (SqlException e)
            {
                throw new Exception("These tests require data from the 'AdventureWorksDW2008R2' SQL Server database to run. You can grab it from here: http://msftdbprodsamples.codeplex.com/", e);
            }

            using (var command = connection.CreateCommand())
            {
                command.CommandText = sql;

                return command.ExecuteReader(CommandBehavior.CloseConnection);
            }
        }

        public static DataTable DataTableFromSql(string sql)
        {
            var dataTable = new DataTable();

            using (var reader = DataReaderFromSql(sql))
            {
                dataTable.Load(reader);
            }

            return dataTable;
        }

        public static DataSet DataSetFromSql(string sql, params string[] tableNames)
        {
            var dataSet = new DataSet();

            using (var reader = DataReaderFromSql(sql))
            {
                dataSet.Load(reader, LoadOption.OverwriteChanges, tableNames);
            }

            return dataSet;
        }

        public static IDataReader ReaderWithMutlipleTables()
        {
            var dataSet = new DataSet
                {
                    Tables =
                        {
                            SmallDataTable(),
                            DifferentSmallDataTable()
                        }
                };

            return dataSet.CreateDataReader();
        }

        public static DataTable GenerateRandomDataTable(int numberOfColumns, int numberOfRows)
        {
            var dataTable = new DataTable();
            for (var i = 0; i < numberOfColumns; i++)
            {
                dataTable.Columns.Add("Column_" + i, typeof(float));
            }

            for (var i = 0; i < numberOfRows; i++)
            {
                var objectArray = Enumerable
                    .Range(0, numberOfColumns)
                    .Select(_ => Random.NextDouble())
                    .Cast<object>()
                    .ToArray();

                dataTable.Rows.Add(objectArray);
            }

            return dataTable;
        }
    }
}
