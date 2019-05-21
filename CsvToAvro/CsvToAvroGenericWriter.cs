using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using Avro;
using Avro.File;
using Avro.Generic;

namespace CsvToAvro
{
    public class CsvToAvroGenericWriter
    {
        public enum Mode
        {
            Create,
            Append
        }

        private const char DEFAULT_SEPARATOR = ',';
        private static RecordSchema _avroSchema;
        private string[] _csvHeaderFields;

        private static DataFileWriter<GenericRecord> _dataFileWriter;

        public static CsvToAvroGenericWriter CreateFromPath(string schemaFilePath, string outputFilePath, Mode mode = Mode.Create)
        {
            string jsonSchema = File.ReadAllText(schemaFilePath, Encoding.UTF8);

            return new CsvToAvroGenericWriter(jsonSchema, outputFilePath, mode);
        }

        public static CsvToAvroGenericWriter CreateFromJson(string jsonSchema, string outputFilePath, Mode mode = Mode.Create)
        {
            return new CsvToAvroGenericWriter(jsonSchema, outputFilePath, mode);
        }

        public static CsvToAvroGenericWriter CreateFromSchema(RecordSchema schema, string outputFilePath, Mode mode = Mode.Create)
        {
            return new CsvToAvroGenericWriter(schema, outputFilePath, mode);
        }

        private CsvToAvroGenericWriter(string jsonSchema, string outputFilePath, Mode mode)
        {
            _avroSchema = (RecordSchema)Schema.Parse(jsonSchema);
            GetDataFileWriter(outputFilePath, mode);
        }

        private CsvToAvroGenericWriter(RecordSchema schema, string outputFilePath, Mode mode)
        {
            _avroSchema = schema;
            GetDataFileWriter(outputFilePath, mode);
        }

        private static void GetDataFileWriter(string outputFilePath, Mode mode)
        {
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(_avroSchema);
            Codec codec = Codec.CreateCodec(Codec.Type.Deflate);

            if (mode == Mode.Create)
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

        public void Append(string[] fields)
        {
            GenericRecord record = Populate(fields);

            IEnumerable<Field> invalidNullFields = GetInvalidNullFields(record);

            if (invalidNullFields.Any())
            {
                throw new Exception(
                    $"There are fields with null, values but the schema does not allow for null: {string.Join(", ", invalidNullFields)}.");
            }

            _dataFileWriter.Append(record);
        }

        public void Append(string line, char separator = DEFAULT_SEPARATOR)
        {
            Append(line.Split(separator));
        }

        //public void Append(string line)
        //{
        //    Append(line.Split(DEFAULT_SEPARATOR));
        //}

        private GenericRecord Populate(string[] fields)
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

            if (nullAllowed)
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

        private bool GetFieldAllowsNull(Field field)
        {
            Schema.Type fieldType = field.Schema.Tag;

            if (fieldType == Schema.Type.Union)
            {
                IList<Schema> schemas = ((UnionSchema) field.Schema).Schemas;

                return schemas.Any(schema => schema.Tag == Schema.Type.Null);
            }

            return false;
        }

        private List<Field> GetInvalidNullFields(GenericRecord record)
        {
            List<Field> avroFields = _avroSchema.Fields;

            var nullFields = new List<Field>();

            foreach (Field field in avroFields)
            {
                if (record.TryGetValue(field.Name, out object value))
                {
                    if (value == null && !GetFieldAllowsNull(field))
                    {
                        nullFields.Add(field);
                    }
                }
            }

            return nullFields;
        }

        public void SetCsvHeader(string[] headerFields)
        {
            _csvHeaderFields = headerFields;
        }

        public void SetCsvHeader(string header, char separator = DEFAULT_SEPARATOR)
        {
            _csvHeaderFields = header.Split(separator);
        }

        //public void SetCsvHeader(string header)
        //{
        //    _csvHeaderFields = header.Split(DEFAULT_SEPARATOR);
        //}

        public void CloseWriter()
        {
            _dataFileWriter.Close();
        }
    }
}