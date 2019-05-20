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

            string schemaFileName = "gpci.avsc";
            string csvFileName = "GPCI2019.csv";

            string[] headerFields = { "medicare_administrative_contractor", "locality_number", "locality_name", "pw_gpci", "pe_gpci", "mp_gpci" };

            string outputFileName = Path.GetFileNameWithoutExtension(csvFileName) + ".avro";

            CsvToAvroGenericWriter writer = new CsvToAvroGenericWriter(baseFolder + schemaFileName, baseFolder + outputFileName, CsvToAvroGenericWriter.MODE_WRITE);

            //writer.SetCsvHeader(headerFields);

            //StreamReader reader = new StreamReader(baseFolder + csvFileName);
            //string line;
            int counter = 0;

            //while ((line = reader.ReadLine()) != null)
            //{
            //    writer.Append(line);
            //    counter++;
            //}

            using (var csvReader = new StringReader(File.ReadAllText(baseFolder + csvFileName)))
            using (var parser = new NotVisualBasic.FileIO.CsvTextFieldParser(csvReader))
            {
                //Skip the header line
                if (!parser.EndOfData)
                {
                    parser.ReadFields();
                }

                while (!parser.EndOfData)
                {
                    string[] csvLine = parser.ReadFields();
                    writer.Append(csvLine);
                    counter++;
                }
            }

            writer.CloseWriter();

            Console.WriteLine($"There were {counter} lines processed.");
        }
    }
}
