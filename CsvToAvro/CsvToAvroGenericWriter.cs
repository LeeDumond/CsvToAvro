using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Avro;
using Avro.File;
using Avro.Generic;

namespace CsvToAvro
{
    internal class CsvToAvroGenericWriter
    {
        public const string SEPARATOR_TAB = "\t";
        public const string SEPARATOR_SEMICOLON = ";";
        public const string SEPARATOR_COMMA = ",";
        public const string SEPARATOR_AT = "@";
        public const string SEPARATOR_AT_AT = "@@";
        public const string SEPARATOR_PIPE = "|";

        // we can overwrite or append
        public const int MODE_WRITE = 0;
        public const int MODE_APPEND = 1;

        // default mode
        private static int mode = MODE_WRITE;

        // default compression factor
        public const int DEFAULT_COMPRESSION = 6;
        //private static Schema NULL_SCHEMA = null;

        private DataFileWriter<GenericRecord> dataFileWriter = null;
        private string outputFileName = null;
        private RecordSchema avroSchema = null;
        private string[] csvHeaderFields = null;
        private Dictionary<string, int> fieldMap = null;
        private string separator = SEPARATOR_SEMICOLON;

        private string csvDateTimeFormat;
        private string csvDateFormat;

        public CsvToAvroGenericWriter(string schemaFilePath, string outputFileName, int mode)
        {
            string json = File.ReadAllText(schemaFilePath, Encoding.UTF8);

            this.avroSchema = (RecordSchema) Schema.Parse(json);
            this.outputFileName = outputFileName;
            CsvToAvroGenericWriter.mode = mode;

            this.GetDataFileWriter(DEFAULT_COMPRESSION);
            this.PopulateFieldMap();
        }

        private void PopulateFieldMap()
        {
            fieldMap = new Dictionary<string, int>();
            List<Field> avroFields = avroSchema.Fields;

            foreach (Field avroField in avroFields)
            {
                fieldMap.Add(avroField.Name, avroField.Pos);
            }
        }

        private void GetDataFileWriter(int compressionFactor)
        {
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(avroSchema);
            Codec codec = Codec.CreateCodec(Codec.Type.Deflate);

            if (mode == MODE_WRITE)
            {
                // create
                this.dataFileWriter = (DataFileWriter<GenericRecord>)DataFileWriter<GenericRecord>.OpenWriter(datumWriter, new FileStream(outputFileName, FileMode.Create), codec);
            }
            else
            {
                this.dataFileWriter = (DataFileWriter<GenericRecord>)DataFileWriter<GenericRecord>.OpenWriter(datumWriter, new FileStream(outputFileName, FileMode.Append), codec);
            }
        }

        public void Append(string line)
        {
            GenericRecord record = Populate(line.Split(','));
            List<Field> nullFields = GetInvalidNullFields(record);

            if (nullFields.Any())
            {
                throw new InvalidOperationException("The following fields have null values: " + string.Join(", ", nullFields));
            }
            else
            {
                dataFileWriter.Append(record);
            }
        }

        private List<Field> GetInvalidNullFields(GenericRecord record)
        {
            throw new NotImplementedException();
        }

        private GenericRecord Populate(string[] fields)
        {
            throw new NotImplementedException();
        }

        public void CloseWriter()
        {
            dataFileWriter.Close();
        }
    }
}