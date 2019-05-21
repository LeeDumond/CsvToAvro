using System;
using System.IO;

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

            CsvToAvroGenericWriter writer = CsvToAvroGenericWriter.CreateFromPath(schemaFilePath, outputFilePath);
            int counter = writer.ConvertFromCsv(csvFilePath);

            //            string jsonSchema = @"{
            //  ""namespace"": ""com.leedumond"",
            //  ""type"": ""record"",
            //  ""name"": ""CostIndex"",
            //  ""version"": ""1"",
            //  ""fields"": [
            //    { ""name"": ""medicare_administrative_contractor"", ""type"": ""string"" },
            //    { ""name"": ""locality_number"", ""type"": ""string"" },
            //    { ""name"": ""locality_name"", ""type"": ""string"" },
            //    { ""name"": ""pw_gpci"", ""type"": ""float"" },
            //    { ""name"": ""pe_gpci"", ""type"": ""float"" },
            //    { ""name"": ""mp_gpci"", ""type"": [""float"", ""null""] }
            //  ]
            //}";
            //            CsvToAvroGenericWriter writer = CsvToAvroGenericWriter.CreateFromJson(jsonSchema, outputFilePath);

            //string[] headerFields = { "medicare_administrative_contractor", "locality_number", "locality_name", "pw_gpci", "pe_gpci", "mp_gpci" };
            //writer.SetCsvHeader(headerFields);

            //using (var reader = new StreamReader(csvFilePath))
            //{
            //    string line;
            //    while ((line = reader.ReadLine()) != null)
            //    {
            //        if (!string.IsNullOrEmpty(line))
            //        {
            //            writer.Append(line);
            //        }
            //    }
            //}

            //writer.CloseWriter();

            Console.WriteLine($"There were {counter} lines processed from: {csvFilePath}");
            Console.WriteLine($"The results were written to: {outputFilePath}");
        }
    }
}
