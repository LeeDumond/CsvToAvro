using System;
using System.IO;
using System.Text;

namespace CsvToAvro
{
    class Program
    {
        static void Main(string[] args)
        {
            string baseFolder = @"C:\Projects\CsvToAvro\CsvToAvro\";

            string outputFileName = "costIndex_test_1.avro";
            string schemaFileName = "costIndex.avsc";

            string csvData_1 = "10112,00,ALABAMA,1.000,0.890,0.492";
            string csvData_2 = "02102,01,ALASKA**,1.500,1.117,0.708";

            

            CsvToAvroGenericWriter writer = new CsvToAvroGenericWriter(baseFolder + schemaFileName, baseFolder + outputFileName, CsvToAvroGenericWriter.MODE_WRITE);

            writer.Append(csvData_1);
            writer.Append(csvData_2);

            writer.CloseWriter();


        }
    }
}
