using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Avro;
using Avro.File;
using Avro.Generic;
using NotVisualBasic.FileIO;

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
        private string _outputFilePath = null;
        private RecordSchema avroSchema = null;
        private string[] csvHeaderFields = null;
        private Dictionary<string, int> fieldMap = null;
        private string separator = SEPARATOR_SEMICOLON;

        private string csvDateTimeFormat;
        private string csvDateFormat;

        public CsvToAvroGenericWriter(string schemaFilePath, string outputFilePath, int mode)
        {
            string json = File.ReadAllText(schemaFilePath, Encoding.UTF8);

            this.avroSchema = (RecordSchema) Schema.Parse(json);
            this._outputFilePath = outputFilePath;
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
                this.dataFileWriter = (DataFileWriter<GenericRecord>)DataFileWriter<GenericRecord>.OpenWriter(datumWriter, new FileStream(_outputFilePath, FileMode.Create));
            }
            else
            {
                this.dataFileWriter = (DataFileWriter<GenericRecord>)DataFileWriter<GenericRecord>.OpenWriter(datumWriter, new FileStream(_outputFilePath, FileMode.Append));
            }
        }

        public void Append(string line)
        {
            //GenericRecord record = Populate(line.Split(','));

            //List<Field> nullFields = GetInvalidNullFields(record);

            //if (nullFields.Any())
            //{
            //    throw new InvalidOperationException("The following fields have null values: " + string.Join(", ", nullFields));
            //}

            //dataFileWriter.Append(record);
        }

        public void Append(string[] fields)
        {
            GenericRecord record = Populate(fields);

            dataFileWriter.Append(record);
        }

        //private List<Field> GetInvalidNullFields(GenericRecord record)
        //{
        //    List<Field> avroFields = avroSchema.Fields;


        //}

        // fields are real values
        private GenericRecord Populate(string[] fields)
        {
            GenericRecord record = new GenericRecord(avroSchema);

            if (csvHeaderFields != null)
            {
                for (int i = 0; i < fields.Length; i++)
                {
                    string csvFieldName = csvHeaderFields[i];

                    if (fieldMap.ContainsKey(csvFieldName))
                    {
                        int avroPosition = fieldMap[csvFieldName];

                        Field field = avroSchema[csvFieldName];

                        object obj = GetObject(field, fields[i]);

                        record.Add(csvFieldName, obj);

                        //record.Add(csvFieldName, fields[i]);
                    }
                }
            }
            else
            {
                List<Field> avroFields = avroSchema.Fields;

                for (int i = 0; i < fields.Length; i++)
                {
                    Field field = avroFields[i];

                    // retrieve a field from the Avro SpecificRecord
                    object obj = GetObject(field, fields[i]);

                    // add the object to the corresponding field
                    record.Add(field.Name, obj);

                    //record.Add(field.Name, fields[i]);
                }


            }

            return record;
        }

        private object GetObject(Field field, string value)
        {
            Schema.Type fieldType = field.Schema.Tag;

            
                switch (fieldType)
                {
                    case Schema.Type.Int:
                        return int.Parse(value);
                    case Schema.Type.Long:
                        return long.Parse(value);
                    case Schema.Type.Float:
                        return float.Parse(value);
                    case Schema.Type.Double:
                        return double.Parse(value);
                    case Schema.Type.Boolean:
                        return float.Parse(value);
                    default:
                        return value;
                }
          

            
        }

        //private Schema.Type GetFieldType(Field field)
        //{
        //    return field.Schema.Tag;
        //}

        public void CloseWriter()
        {
            
            dataFileWriter.Close();
        }

        public void SetCsvHeader(string[] headerFields)
        {
            this.csvHeaderFields = headerFields;
        }
    }
}