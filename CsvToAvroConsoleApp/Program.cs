using System;
using System.IO;
using CsvToAvro;

namespace CsvToAvroConsoleApp
{
    class Program
    {
        private static string schemaFilePath = @"C:\CsvToAvro\mySchema.avsc";
        private static string csvFilePath = @"C:\CsvToAvro\myCsv.csv";
        private static string outputFilePath = @"C:\CsvToAvro\myAvro.avro";

        static void Main(string[] args)
        {
            // uncomment whichever scenario you wish to demonstrate below

            BasicUsage();
            // AdvancedUsage();
        }

        private static void BasicUsage()
        {
            CsvToAvroGenericWriter writer = CsvToAvroGenericWriter.CreateFromPath(schemaFilePath, outputFilePath);

            int counter = writer.ConvertFromCsv(csvFilePath);

            Console.WriteLine($"There were {counter} lines processed from: {csvFilePath}");
            Console.WriteLine($"The results were written to: {outputFilePath}");
        }

        private static void AdvancedUsage()
        {
            string jsonSchema = @"{
              ""namespace"": ""com.leedumond"",
              ""type"": ""record"",
              ""name"": ""CostIndex"",
              ""version"": ""1"",
              ""fields"": [
                { ""name"": ""medicare_administrative_contractor"", ""type"": ""string"" },
                { ""name"": ""locality_number"", ""type"": ""string"" },
                { ""name"": ""locality_name"", ""type"": ""string"" },
                { ""name"": ""pw_gpci"", ""type"": ""float"" },
                { ""name"": ""pe_gpci"", ""type"": ""float"" },
                { ""name"": ""mp_gpci"", ""type"": [""float"", ""null""] }
              ]
            }";

            CsvToAvroGenericWriter writer = CsvToAvroGenericWriter.CreateFromJson(jsonSchema, outputFilePath);

            string[] headerFields = { "medicare_administrative_contractor", "locality_number", "locality_name", "pw_gpci", "pe_gpci", "mp_gpci" };
            writer.SetCsvHeader(headerFields);

            int counter = 0;

            using (var reader = new StreamReader(csvFilePath))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    if (!string.IsNullOrEmpty(line))
                    {
                        writer.Append(line);
                        counter++;
                    }
                }
            }

            writer.Dispose();

            Console.WriteLine($"There were {counter} lines processed from: {csvFilePath}");
            Console.WriteLine($"The results were written to: {outputFilePath}");
        }
    }
}
