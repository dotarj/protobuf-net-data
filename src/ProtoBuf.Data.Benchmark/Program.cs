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
