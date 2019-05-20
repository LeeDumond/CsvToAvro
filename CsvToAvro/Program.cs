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
            string csvData_2 = "02102,01,ALASKA,1.500,1.117,0.708";
            string csvData_3 = "03102,00,ARIZONA,1.000,0.971,0.834";
            string csvData_4 = "07102,13,ARKANSAS,1.000,0.872,0.576";
            string csvData_5 = "01112,54,BAKERSFIELD,1.020,1.074,0.618";

            string[] headerFields = { "medicare_administrative_contractor", "locality_number", "locality_name", "pw_gpci", "pe_gpci", "mp_gpci" };

            CsvToAvroGenericWriter writer = new CsvToAvroGenericWriter(baseFolder + schemaFileName, baseFolder + outputFileName, CsvToAvroGenericWriter.MODE_WRITE);

            //writer.SetCsvHeader(headerFields);

            writer.Append(csvData_1);
            writer.Append(csvData_2);
            writer.Append(csvData_3);
            writer.Append(csvData_4);
            writer.Append(csvData_5);

            writer.CloseWriter();
        }
    }
}
