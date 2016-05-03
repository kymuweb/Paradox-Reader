using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Text;
using ParadoxReader;

namespace ParadoxTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var dbPath = Path.Combine(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location), "data");

            Console.WriteLine("Test 1: sequential read first 10 records from start");
            Console.WriteLine("==========================================================");
            var table = new ParadoxTable(dbPath, "zakazky");
            var recIndex = 1;
            foreach (var rec in table.Enumerate())
            {
                Console.WriteLine("Record #{0}", recIndex++);
                for (int i=0; i<table.FieldCount; i++)
                {
                    Console.WriteLine("    {0} = {1}", table.FieldNames[i], rec.DataValues[i]);
                }
                if (recIndex > 10) break;
            }
            Console.WriteLine("-- press [enter] to continue --");
            Console.ReadKey();
            Console.Clear();

            Console.WriteLine("Test 2: read 10 records by index (key range: 1750 -> 1760)");
            Console.WriteLine("==========================================================");

            var index = new ParadoxPrimaryKey(table, Path.Combine(dbPath, "zakazky.PX"));
            var condition =
                new ParadoxCondition.LogicalAnd(
                    new ParadoxCondition.Compare(ParadoxCompareOperator.GreaterOrEqual, 1750, 0, 0),
                    new ParadoxCondition.Compare(ParadoxCompareOperator.LessOrEqual, 1760, 0, 0));
            var qry = index.Enumerate(condition);
            var rdr = new ParadoxDataReader(table, qry);
            recIndex = 1;
            while (rdr.Read())
            {
                Console.WriteLine("Record #{0}", recIndex++);
                for (int i = 0; i < rdr.FieldCount; i++)
                {
                    Console.WriteLine("    {0} = {1}", rdr.GetName(i), rdr[i]);
                }
            }
            Console.WriteLine("-- press [enter] to continue --");
            Console.ReadKey();

        }
    }
}
