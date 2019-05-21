using System;
using System.IO;
using System.Text;
using NotVisualBasic.FileIO;

namespace CsvToAvro
{
    class Program
    {
        static void Main(string[] args)
        {
            string baseFolder = @"C:\Projects\CsvToAvro\CsvToAvro\";

            string schemaFileName = "gpci.avsc";
            string csvFileName = "GPCI2019.csv";

            string csvFilePath = baseFolder + csvFileName;
            string schemaFilePath = baseFolder + schemaFileName;

            string outputFileName = Path.GetFileNameWithoutExtension(csvFileName) + ".avro";

            string outputFilePath = baseFolder + outputFileName;

            CsvToAvroGenericWriter writer = new CsvToAvroGenericWriter(schemaFilePath, outputFilePath, CsvToAvroGenericWriter.MODE_WRITE);
            string[] headerFields = { "medicare_administrative_contractor", "locality_number", "locality_name", "pw_gpci", "pe_gpci", "mp_gpci" };
            writer.SetCsvHeader(headerFields);


            int counter = 0;
            
            using (var parser = new CsvTextFieldParser(csvFilePath))
            {
                while (!parser.EndOfData)
                {
                    string[] csvValues = parser.ReadFields();
                    writer.Append(csvValues);
                    counter++;
                }
            }

            writer.CloseWriter();

            Console.WriteLine($"There were {counter} lines processed from: {csvFilePath}");
            Console.WriteLine($"The results were written to: {outputFilePath}");
        }
    }
}
