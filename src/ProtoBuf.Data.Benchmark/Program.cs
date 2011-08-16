using System;
using System.Data.SqlClient;

namespace ProtoBuf.Data.Benchmark
{
    class Program
    {
        static void Main()
        {
            try
            {
                // This benchmark requires the AdventureWorks DW database. You
                // can grab it from here: http://msftdbprodsamples.codeplex.com/
                var connectionString = new SqlConnectionStringBuilder
                                           {
                                               DataSource = @".\SQLEXPRESS",
                                               InitialCatalog = "AdventureWorksDW2008R2",
                                               IntegratedSecurity = true
                                           };

                new Benchmark(connectionString.ConnectionString).Run();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                throw;
            }

            Console.ReadLine();
        }
    }
}
