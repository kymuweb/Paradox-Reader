using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using ParadoxReader;

namespace ParadoxTest
{
    internal class Program
    {


        static void Main(string[] args)
        {

            var dbPath = ParadoxTest.Configuration.GetParadoxDataFolderPath("Test");

            if (!Directory.Exists(dbPath))
            {
                throw new DirectoryNotFoundException($"Could not find {dbPath}");
            }

            var testTablePath = string.Empty;

            var dbTableFilePaths = Directory.GetFiles(dbPath, "*.DB");
            var dbTableFilePathsCount = dbTableFilePaths?.Length ?? 0;

            //var desiredTestTableName = "TESTTAB.DB";
            var desiredTestTableName = "TESTTABNOINDEX.DB";

            testTablePath = dbTableFilePaths.FirstOrDefault(path => path.IndexOf(desiredTestTableName, StringComparison.OrdinalIgnoreCase) >= 0);

            if (string.IsNullOrWhiteSpace(testTablePath) && dbTableFilePathsCount > 0)
            {
                Random r = new Random();
                testTablePath = dbTableFilePaths[r.Next(0, dbTableFilePathsCount)];
            }

            if (string.IsNullOrWhiteSpace(testTablePath) || !File.Exists(testTablePath))
            {
                throw new FileNotFoundException($"Could not find .DB file in {dbPath}");
            }

            Console.WriteLine("Test 1: sequential read first 10 records from start");
            Console.WriteLine("==========================================================");
            using (var table = new ParadoxTable(dbPath, Path.GetFileName(testTablePath)))
            {
                var recIndex = 1;
                foreach (var rec in table.Enumerate())
                {
                    Console.WriteLine("Record #{0}", recIndex);
                    for (int i = 0; i < table.FieldCount; i++)
                    {
                        var fieldName = table.FieldNames[i] ?? string.Empty;
                        var dataValue = rec.DataValues[i];
                        var dataValueToStr = dataValue?.ToString() ?? string.Empty;
                        if(dataValue != null && dataValue is byte[])
                        {
                            dataValueToStr = Convert.ToBase64String((byte[])dataValue);
                        }
                        Console.WriteLine("    {0} = {1}", fieldName, dataValueToStr);
                    }


                    var now = DateTime.Now;

                    var tmpDataValues = new object[rec.DataValues.Length];
                    for (int i = 0; i < rec.DataValues.Length; i++)
                    {
                        var value = rec.DataValues[i];
                        if (value is ICloneable cloneable)
                            tmpDataValues[i] = cloneable.Clone();
                        else if (value is byte[] bytes)
                            tmpDataValues[i] = (byte[])bytes.Clone();
                        else
                            tmpDataValues[i] = value; // Value types and immutable types (like string)
                    }

                    tmpDataValues[0] = 9; // 9;
                    tmpDataValues[1] = "ZZZ"; // ZZZ
                    tmpDataValues[2] = 999.99d;
                    tmpDataValues[3] = 999.99d;
                    tmpDataValues[4] = (short)999;
                    tmpDataValues[5] = 999;
                    tmpDataValues[6] = 999.99d; // 999.99M;
                    tmpDataValues[7] = now;
                    tmpDataValues[8] = now.TimeOfDay;
                    tmpDataValues[9] = now;
                    tmpDataValues[10] = false;
                    //tmpDataValues[11] = new byte[] { (byte)9 };
                    //tmpDataValues[12] = new byte[] { (byte)9 };
                    //tmpDataValues[13] = "ZZZ";

                    Console.WriteLine("Setting Record #{0}", recIndex);

                    rec.DataValues = tmpDataValues;

                    Console.WriteLine("Re-reading Record #{0}", recIndex);
                    for (int i = 0; i < table.FieldCount; i++)
                    {
                        var fieldName = table.FieldNames[i] ?? string.Empty;
                        var dataValue = rec.DataValues[i];
                        var dataValueToStr = dataValue?.ToString() ?? string.Empty;
                        if (dataValue != null && dataValue is byte[])
                        {
                            dataValueToStr = Convert.ToBase64String((byte[])dataValue);
                        }
                        Console.WriteLine("    {0} = {1}", fieldName, dataValueToStr);
                    }


                    
                    if (++recIndex > 1) break;
                }





                Console.WriteLine("-- press any key to continue --");
                Console.ReadKey();
                //Console.Clear();

                //Console.WriteLine("Test 2: read 10 records by index (key range: 3 -> 4)");
                //Console.WriteLine("==========================================================");

                //using (var index = table.PrimaryKeyIndex)
                //{
                //    if (index != null)
                //    {
                //        var condition =
                //            new ParadoxCondition.LogicalAnd(
                //                new ParadoxCondition.Compare(ParadoxCompareOperator.GreaterOrEqual, 3, 0, 0),
                //                new ParadoxCondition.Compare(ParadoxCompareOperator.LessOrEqual, 4, 0, 0));
                //        var qry = index.Enumerate(condition);
                //        using (var rdr = new ParadoxDataReader(table, qry))
                //        {
                //            recIndex = 1;
                //            while (rdr.Read())
                //            {
                //                Console.WriteLine("Record #{0}", recIndex);
                //                for (int i = 0; i < rdr.FieldCount; i++)
                //                {
                //                    Console.WriteLine("    {0} = {1}", rdr.GetName(i), rdr[i]);
                //                }

                //                if (++recIndex > 10) { break; }
                //            }
                //        }
                //    }
                //}
            }

            Console.WriteLine("-- press any key to continue --");
            Console.ReadKey();

        }
    }
}
