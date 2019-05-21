﻿using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.IO;
using System.Text;
using Avro;
using Avro.File;
using Avro.Generic;

namespace CsvToAvro
{
    public class CsvToAvroGenericWriter
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
        //public const int DEFAULT_COMPRESSION = 6;
        //private static Schema NULL_SCHEMA = null;

        private DataFileWriter<GenericRecord> dataFileWriter = null;
        private string _outputFilePath = null;
        private RecordSchema avroSchema = null;
        private string[] csvHeaderFields = null;
        //private Dictionary<string, int> fieldMap = null;
        private string separator = SEPARATOR_SEMICOLON;

        private string csvDateTimeFormat;
        private string csvDateFormat;

        public CsvToAvroGenericWriter(string schemaFilePath, string outputFilePath, int mode)
        {
            string json = File.ReadAllText(schemaFilePath, Encoding.UTF8);

            this.avroSchema = (RecordSchema) Schema.Parse(json);
            this._outputFilePath = outputFilePath;
            CsvToAvroGenericWriter.mode = mode;

            this.GetDataFileWriter();
            //this.PopulateFieldMap();
        }

        //private void PopulateFieldMap()
        //{
        //    fieldMap = new Dictionary<string, int>();
        //    List<Field> avroFields = avroSchema.Fields;

        //    foreach (Field avroField in avroFields)
        //    {
        //        fieldMap.Add(avroField.Name, avroField.Pos);
        //    }
        //}

        private void GetDataFileWriter()
        {
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(avroSchema);
            Codec codec = Codec.CreateCodec(Codec.Type.Deflate);

            if (mode == MODE_WRITE)
            {
                // create
                this.dataFileWriter =
                    (DataFileWriter<GenericRecord>) DataFileWriter<GenericRecord>.OpenWriter(datumWriter,
                        new FileStream(_outputFilePath, FileMode.Create), codec);
            }
            else
            {
                this.dataFileWriter =
                    (DataFileWriter<GenericRecord>) DataFileWriter<GenericRecord>.OpenWriter(datumWriter,
                        new FileStream(_outputFilePath, FileMode.Append), codec);
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
            List<Field> avroFields = avroSchema.Fields;

            if (csvHeaderFields != null)
            {
                for (int i = 0; i < fields.Length; i++)
                {
                    string csvFieldName = csvHeaderFields[i];
                    Field field = avroSchema[csvFieldName];

                    if (field != null)
                    {
                        //Field field = avroSchema[csvFieldName];

                        object obj = GetObject(field, fields[i]);

                        record.Add(csvFieldName, obj);
                    }
                }
            }
            else
            {
                //List<Field> avroFields = avroSchema.Fields;

                for (int i = 0; i < fields.Length; i++)
                {
                    Field field = avroFields[i];

                    // retrieve a field from the Avro SpecificRecord
                    object obj = GetObject(field, fields[i]);

                    // add the object to the corresponding field
                    record.Add(field.Name, obj);
                }
            }

            return record;
        }

        private object GetObject(Field field, string value)
        {
            Schema.Type fieldType = GetFieldType(field);

            bool nullAllowed = GetFieldAllowsNull(field);

            

            if (!string.IsNullOrWhiteSpace(value))
            {
                switch (fieldType)
                {
                    case Schema.Type.Int:
                        try
                        {
                            return int.Parse(value);
                        }
                        catch (Exception ex) when (ex is FormatException || ex is OverflowException)
                        {
                            throw new InvalidOperationException(GetParseExceptionMessage(value, field.Name, typeof(int)));
                        }
                    case Schema.Type.Long:
                        try
                        {
                            return long.Parse(value);
                        }
                        catch (Exception ex) when (ex is FormatException || ex is OverflowException)
                        {
                            throw new InvalidOperationException(GetParseExceptionMessage(value, field.Name, typeof(long)));
                        }
                    case Schema.Type.Float:
                        try
                        {
                            return float.Parse(value);
                        }
                        catch (Exception ex) when (ex is FormatException || ex is OverflowException)
                        {
                            throw new InvalidOperationException(GetParseExceptionMessage(value, field.Name, typeof(float)));
                        }
                    case Schema.Type.Double:
                        try
                        {
                            return double.Parse(value);
                        }
                        catch (Exception ex) when (ex is FormatException || ex is OverflowException)
                        {
                            throw new InvalidOperationException(GetParseExceptionMessage(value, field.Name, typeof(double)));
                        }
                    case Schema.Type.Boolean:
                        try
                        {
                            return bool.Parse(value);
                        }
                        catch (Exception ex) when (ex is FormatException || ex is OverflowException)
                        {
                            throw new InvalidOperationException(GetParseExceptionMessage(value, field.Name, typeof(bool)));
                        }
                    case Schema.Type.String:
                        return value;
                    default:
                        throw new NotSupportedException($"Type {fieldType} is not supported for field '{field.Name}'.");
                }
                
            }

            if (nullAllowed)
            {
                return null;
            }

            throw new InvalidOperationException($"Value of 'null' is not allowed for field '{field.Name}'.");
        }

        private Schema.Type GetFieldType(Field field)
        {
            Schema.Type fieldType = field.Schema.Tag;

            if (fieldType == Schema.Type.Union)
            {
                IList<Schema> types = ((UnionSchema)field.Schema).Schemas;

                foreach (Schema schema in types)
                {
                    if (schema.Tag != Schema.Type.Null)
                    {
                        return schema.Tag;
                    }
                }

                return fieldType;
            }

            return fieldType;
        }


        private bool GetFieldAllowsNull(Field field)
        {
            Schema.Type fieldType = field.Schema.Tag;

            if (fieldType == Schema.Type.Union)
            {
                IList<Schema> types = ((UnionSchema) field.Schema).Schemas;

                foreach (Schema schema in types)
                {
                    if (schema.Tag == Schema.Type.Null)
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        private string GetParseExceptionMessage(string value, string fieldName, Type type)
        {
            return $"Value '{value}' of field '{fieldName}' could not be converted to a {type.FullName}.";
        }


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