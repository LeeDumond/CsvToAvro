﻿using System;
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
    public class CsvToAvroGenericWriter
    {
        public enum Mode
        {
            /// <summary>
            /// Specifies that the operating system should create a new file. If the file already exists, it will be overwritten.
            /// This requires Write permission. If the file already exists but is a hidden file, an UnauthorizedAccessException
            /// exception is thrown.
            /// </summary>
            Overwrite,
            /// <summary>
            /// Opens the file if it exists and seeks to the end of the file, or creates a new file. This requires Append permission. 
            /// </summary>
            Append
        }

        private const char DEFAULT_SEPARATOR = ',';
        private static RecordSchema _avroSchema;

        private static DataFileWriter<GenericRecord> _dataFileWriter;
        private string[] _csvHeaderFields;

        private CsvToAvroGenericWriter(string jsonSchema, string outputFilePath, Mode mode)
        {
            _avroSchema = (RecordSchema) Schema.Parse(jsonSchema);
            BuildDataFileWriter(outputFilePath, mode);
        }

        private CsvToAvroGenericWriter(RecordSchema schema, string outputFilePath, Mode mode)
        {
            _avroSchema = schema;
            BuildDataFileWriter(outputFilePath, mode);
        }

        private static void BuildDataFileWriter(string outputFilePath, Mode mode)
        {
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(_avroSchema);
            Codec codec = Codec.CreateCodec(Codec.Type.Deflate);

            if (mode == Mode.Overwrite)
            {
                _dataFileWriter =
                    (DataFileWriter<GenericRecord>) DataFileWriter<GenericRecord>.OpenWriter(datumWriter,
                        new FileStream(outputFilePath, FileMode.Create), codec);
            }
            else
            {
                _dataFileWriter =
                    (DataFileWriter<GenericRecord>) DataFileWriter<GenericRecord>.OpenWriter(datumWriter,
                        new FileStream(outputFilePath, FileMode.Append), codec);
            }
        }

        /// <summary>
        /// Gets an instance of CsvToAvroGenericWriter.
        /// </summary>
        /// <param name="schemaFilePath">The path to the file containing the Avro schema as properly formatted JSON.</param>
        /// <param name="outputFilePath">The path the Avro file should be written to.</param>
        /// <param name="mode">If the output Avro file already exists, specified whether it should be overwritten or appended to.
        /// The default is Overwrite.</param>
        /// <returns>A CsvToAvroGenericWriter object.</returns>
        public static CsvToAvroGenericWriter CreateFromPath(string schemaFilePath, string outputFilePath,
            Mode mode = Mode.Overwrite)
        {
            string jsonSchema = File.ReadAllText(schemaFilePath, Encoding.UTF8);

            return new CsvToAvroGenericWriter(jsonSchema, outputFilePath, mode);
        }

        /// <summary>
        /// Gets an instance of CsvToAvroGenericWriter.
        /// </summary>
        /// <param name="jsonSchema">A string containing the schema as properly formatted JSON.</param>
        /// <param name="outputFilePath">The path the Avro file should be written to.</param>
        /// <param name="mode">If the output Avro file already exists, specified whether it should be overwritten or appended to.
        /// The default is Overwrite.</param>
        /// <returns>A CsvToAvroGenericWriter object.</returns>
        public static CsvToAvroGenericWriter CreateFromJson(string jsonSchema, string outputFilePath,
            Mode mode = Mode.Overwrite)
        {
            return new CsvToAvroGenericWriter(jsonSchema, outputFilePath, mode);
        }

        /// <summary>
        /// Gets an instance of CsvToAvroGenericWriter.
        /// </summary>
        /// <param name="schema">An Avro RecordSchema object containing the schema.</param>
        /// <param name="outputFilePath">The path the Avro file should be written to.</param>
        /// <param name="mode">If the output Avro file already exists, specified whether it should be overwritten or appended to.
        /// The default is Overwrite.</param>
        /// <returns>A CsvToAvroGenericWriter object.</returns>
        public static CsvToAvroGenericWriter CreateFromSchema(RecordSchema schema, string outputFilePath,
            Mode mode = Mode.Overwrite)
        {
            return new CsvToAvroGenericWriter(schema, outputFilePath, mode);
        }

        /// <summary>
        /// Sets the list of CSV headers.
        /// </summary>
        /// <param name="headerFields">An array containing the field names in the order in which the fields appear in the CSV data.</param>
        public void SetCsvHeader(string[] headerFields)
        {
            _csvHeaderFields = headerFields;
        }

        /// <summary>
        /// Sets the list of CSV headers.
        /// </summary>
        /// <param name="header">An separated string containing the field names in the order in which the fields appear in the CSV data.</param>
        /// <param name="separator">The separator used in the supplied string. The default is comma (',').</param>
        public void SetCsvHeader(string header, char separator = DEFAULT_SEPARATOR)
        {
            _csvHeaderFields = header.Split(separator);
        }

        /// <summary>
        /// Converts a CSV file to a file in Avro format, using the schema, output path, and mode specified by the writer.
        /// </summary>
        /// <param name="csvFilePath">The path to a text file containing the CSV data.</param>
        /// <param name="headerLinesToSkip">The number of lines to skip from the beginning of the CSV file.</param>
        /// <param name="separator">The separator used by the supplied CSV data.</param>
        /// <returns>The number of lines processed from the supplied file..</returns>
        public int ConvertFromCsv(string csvFilePath, int headerLinesToSkip = 0, char separator = DEFAULT_SEPARATOR)
        {
            return ConvertFromCsv(new CsvTextFieldParser(csvFilePath), headerLinesToSkip, separator);
        }

        /// <summary>
        /// Converts a CSV file to a file in Avro format, using the schema, output path, and mode specified by the writer.
        /// </summary>
        /// <param name="reader">A TextReader containing the CSV data.</param>
        /// <param name="headerLinesToSkip">The number of lines to skip from the beginning of the CSV file.</param>
        /// <param name="separator">The separator used by the supplied CSV data.</param>
        /// <returns>The number of lines processed from the supplied TextReader.</returns>
        public int ConvertFromCsv(TextReader reader, int headerLinesToSkip = 0, char separator = DEFAULT_SEPARATOR)
        {
            return ConvertFromCsv(new CsvTextFieldParser(reader), headerLinesToSkip, separator);
        }

        private int ConvertFromCsv(CsvTextFieldParser parser, int headerLinesToSkip, char separator)
        {
            int counter = 0;

            using (parser)
            {
                parser.SetDelimiter(separator);
                parser.TrimWhiteSpace = false;

                for (int i = 0; i < headerLinesToSkip; i++)
                {
                    if (!parser.EndOfData)
                    {
                        parser.ReadFields();
                    }
                }

                while (!parser.EndOfData)
                {
                    string[] csvValues = parser.ReadFields();
                    Append(csvValues);
                    counter++;
                }
            }

            CloseWriter();

            return counter;
        }

        /// <summary>
        /// Appends data to the end of the Avro file currently being written to. 
        /// </summary>
        /// <param name="fields">An array of strings containing the data to be appended.</param>
        public void Append(string[] fields)
        {
            GenericRecord record = GetGenericRecord(fields);

            IEnumerable<Field> invalidNullFields = GetInvalidNullFields(record);

            if (invalidNullFields.Any())
            {
                throw new Exception(
                    $"There are fields with null, values but the schema does not allow for null: {string.Join(", ", invalidNullFields)}.");
            }

            _dataFileWriter.Append(record);
        }

        /// <summary>
        /// Appends data to the end of the Avro file currently being written to. 
        /// </summary>
        /// <param name="line">An separated string containing the data to be appended.
        /// No attempt is made to perform any complex parsing; it simply splits the string based on the supplied separator.
        /// If your line data has quoted values, newlines, escaped quotes, etc. you should use the other Append method.
        /// </param>
        /// <param name="separator">The separator used in the supplied string. The default is comma (',').</param>
        public void Append(string line, char separator = DEFAULT_SEPARATOR)
        {
            Append(line.Split(separator));
        }

        /// <summary>
        /// Writes the Avro header metadata, closes the filestream, and cleans up resources.
        /// </summary>
        public void CloseWriter()
        {
            _dataFileWriter.Close();
        }

        private GenericRecord GetGenericRecord(string[] fields)
        {
            var record = new GenericRecord(_avroSchema);
            List<Field> avroFields = _avroSchema.Fields;

            if (_csvHeaderFields != null)
            {
                for (var i = 0; i < fields.Length; i++)
                {
                    string csvFieldName = _csvHeaderFields[i];
                    Field field = _avroSchema[csvFieldName];

                    if (field != null)
                    {
                        object obj = GetObject(field, fields[i]);

                        record.Add(csvFieldName, obj);
                    }
                }
            }
            else
            {
                for (var i = 0; i < fields.Length; i++)
                {
                    Field field = avroFields[i];

                    object obj = GetObject(field, fields[i]);

                    record.Add(field.Name, obj);
                }
            }

            return record;
        }

        private object GetObject(Field field, string value)
        {
            Schema.Type fieldType = GetFieldType(field);

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
                            throw new InvalidOperationException(
                                GetParseExceptionMessage(value, field.Name, typeof(int)));
                        }

                    case Schema.Type.Long:
                        try
                        {
                            return long.Parse(value);
                        }
                        catch (Exception ex) when (ex is FormatException || ex is OverflowException)
                        {
                            throw new InvalidOperationException(GetParseExceptionMessage(value, field.Name,
                                typeof(long)));
                        }

                    case Schema.Type.Float:
                        try
                        {
                            return float.Parse(value);
                        }
                        catch (Exception ex) when (ex is FormatException || ex is OverflowException)
                        {
                            throw new InvalidOperationException(GetParseExceptionMessage(value, field.Name,
                                typeof(float)));
                        }

                    case Schema.Type.Double:
                        try
                        {
                            return double.Parse(value);
                        }
                        catch (Exception ex) when (ex is FormatException || ex is OverflowException)
                        {
                            throw new InvalidOperationException(GetParseExceptionMessage(value, field.Name,
                                typeof(double)));
                        }

                    case Schema.Type.Boolean:
                        try
                        {
                            return bool.Parse(value);
                        }
                        catch (Exception ex) when (ex is FormatException || ex is OverflowException)
                        {
                            throw new InvalidOperationException(GetParseExceptionMessage(value, field.Name,
                                typeof(bool)));
                        }

                    case Schema.Type.String:
                        return value;
                    default:
                        throw new NotSupportedException($"Type {fieldType} is not supported for field '{field.Name}'.");
                }
            }

            if (FieldAllowsNull(field))
            {
                return null;
            }

            throw new InvalidOperationException($"Value of 'null' is not allowed for field '{field.Name}'.");
        }

        private string GetParseExceptionMessage(string value, string fieldName, Type type)
        {
            return $"Value '{value}' of field '{fieldName}' could not be converted to a {type.FullName}.";
        }

        private Schema.Type GetFieldType(Field field)
        {
            Schema.Type fieldType = field.Schema.Tag;

            if (fieldType == Schema.Type.Union)
            {
                IList<Schema> types = ((UnionSchema) field.Schema).Schemas;

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

        private bool FieldAllowsNull(Field field)
        {
            Schema.Type fieldType = field.Schema.Tag;

            if (fieldType != Schema.Type.Union)
            {
                return false;
            }

            IList<Schema> schemas = ((UnionSchema) field.Schema).Schemas;

            return schemas.Any(schema => schema.Tag == Schema.Type.Null);
        }

        private List<Field> GetInvalidNullFields(GenericRecord record)
        {
            List<Field> avroFields = _avroSchema.Fields;

            var invalidNullFields = new List<Field>();

            foreach (Field field in avroFields)
            {
                if (record.TryGetValue(field.Name, out object value))
                {
                    if (value == null && !FieldAllowsNull(field))
                    {
                        invalidNullFields.Add(field);
                    }
                }
            }

            return invalidNullFields;
        }
    }
}