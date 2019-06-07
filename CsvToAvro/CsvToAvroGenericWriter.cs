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
    public class CsvToAvroGenericWriter : IDisposable
    {
        public enum Mode
        {
            /// <summary>
            ///     If the file already exists, overwrites the file; otherwise, creates a new file.
            ///     This requires Write permission. If the file already exists but is a hidden file,
            ///     an UnauthorizedAccessException exception is thrown.
            /// </summary>
            Overwrite,

            /// <summary>
            ///     If the file already exists, opens the file and seeks to the end of the file; otherwise, creates a new file.
            ///     This requires Append permission.
            /// </summary>
            Append
        }

        private const char DEFAULT_SEPARATOR = ',';
        private static readonly Encoding DEFAULT_ENCODING = Encoding.UTF8;
        private static RecordSchema _avroSchema;
        private static DataFileWriter<GenericRecord> _dataFileWriter;
        private string[] _csvHeaderFields;

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
                _dataFileWriter = (DataFileWriter<GenericRecord>) DataFileWriter<GenericRecord>.OpenWriter(datumWriter,
                    new FileStream(outputFilePath, FileMode.Create), codec);
            }
            else
            {
                _dataFileWriter = (DataFileWriter<GenericRecord>) DataFileWriter<GenericRecord>.OpenWriter(datumWriter,
                    new FileStream(outputFilePath, FileMode.Append), codec);
            }
        }

        /// <summary>
        ///     Gets an instance of CsvToAvroGenericWriter.
        /// </summary>
        /// <param name="schemaFilePath">
        ///     The path to the file containing the Avro schema as properly formatted JSON. This overload
        ///     assumes UTF8 encoding for the file.
        /// </param>
        /// <param name="outputFilePath">The path the Avro file should be written to.</param>
        /// <param name="mode">
        ///     If the output Avro file already exists, specified whether it should be overwritten or appended to.
        ///     The default is Overwrite.
        /// </param>
        /// <returns>A CsvToAvroGenericWriter object.</returns>
        /// <exception cref="ArgumentException">
        ///     <paramref name="schemaFilePath">schemaFilePath</paramref> is an empty string (""),
        ///     contains only white space, or contains one or more invalid characters.
        /// </exception>
        /// <exception cref="ArgumentNullException"><paramref name="schemaFilePath">schemaFilePath</paramref> is null.</exception>
        /// <exception cref="ArgumentException">
        ///     The contents of the schema file is an empty string (""), or contains only white
        ///     space.
        /// </exception>
        /// <exception cref="SchemaParseException">The contents of the schema file could not correctly converted into valid JSON.</exception>
        /// <exception cref="ArgumentException">
        ///     <paramref name="outputFilePath">outputFilePath</paramref> is an empty string (""),
        ///     contains only white space, or contains one or more invalid characters, or refers to a non-file device, such as
        ///     "con:", "com1:", "lpt1:", etc. in an NTFS environment.
        /// </exception>
        /// <exception cref="NotSupportedException">
        ///     <paramref name="outputFilePath">outputFilePath</paramref> refers to a non-file
        ///     device, such as "con:", "com1:", "lpt1:", etc. in a non-NTFS environment.
        /// </exception>
        /// <exception cref="ArgumentNullException"><paramref name="outputFilePath">outputFilePath</paramref> is null.</exception>
        /// <exception cref="System.Security.SecurityException">The caller does not have the required permission.</exception>
        /// <exception cref="IOException">
        ///     An I/O error has occured while opening the source file, or the underlying stream has been
        ///     unexpectedly closed.
        /// </exception>
        /// <exception cref="DirectoryNotFoundException">The specified path is invalid, such as being on an unmapped drive.</exception>
        /// <exception cref="PathTooLongException">
        ///     The specified path, file name, or both exceed the system-defined maximum length.
        ///     For example, on Windows-based platforms, paths must be less than 248 characters, and file names must be less than
        ///     260 characters.
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="mode">mode</paramref> contains an invalid value.</exception>
        public static CsvToAvroGenericWriter CreateFromPath(string schemaFilePath, string outputFilePath,
            Mode mode = Mode.Overwrite)
        {
            return CreateFromPath(schemaFilePath, outputFilePath, DEFAULT_ENCODING, mode);
        }

        /// <summary>
        ///     Gets an instance of CsvToAvroGenericWriter.
        /// </summary>
        /// <param name="schemaFilePath">The path to the file containing the Avro schema as properly formatted JSON.</param>
        /// <param name="outputFilePath">The path the Avro file should be written to.</param>
        /// <param name="encoding">The encoding used for the file containing the Avro schema.</param>
        /// <param name="mode">
        ///     If the output Avro file already exists, specified whether it should be overwritten or appended to.
        ///     The default is Overwrite.
        /// </param>
        /// <returns>A CsvToAvroGenericWriter object.</returns>
        /// <exception cref="ArgumentException">
        ///     <paramref name="schemaFilePath">schemaFilePath</paramref> is an empty string (""),
        ///     contains only white space, or contains one or more invalid characters.
        /// </exception>
        /// <exception cref="ArgumentNullException"><paramref name="schemaFilePath">schemaFilePath</paramref> is null.</exception>
        /// <exception cref="ArgumentException">
        ///     The contents of the schema file is an empty string (""), or contains only white
        ///     space.
        /// </exception>
        /// <exception cref="SchemaParseException">The contents of the schema file could not correctly converted into valid JSON.</exception>
        /// <exception cref="ArgumentException">
        ///     <paramref name="outputFilePath">outputFilePath</paramref> is an empty string (""),
        ///     contains only white space, or contains one or more invalid characters, or refers to a non-file device, such as
        ///     "con:", "com1:", "lpt1:", etc. in an NTFS environment.
        /// </exception>
        /// <exception cref="NotSupportedException">
        ///     <paramref name="outputFilePath">outputFilePath</paramref> refers to a non-file
        ///     device, such as "con:", "com1:", "lpt1:", etc. in a non-NTFS environment.
        /// </exception>
        /// <exception cref="ArgumentNullException"><paramref name="outputFilePath">outputFilePath</paramref> is null.</exception>
        /// <exception cref="System.Security.SecurityException">The caller does not have the required permission.</exception>
        /// <exception cref="IOException">
        ///     An I/O error has occured while opening the source file, or the underlying stream has been
        ///     unexpectedly closed.
        /// </exception>
        /// <exception cref="DirectoryNotFoundException">The specified path is invalid, such as being on an unmapped drive.</exception>
        /// <exception cref="PathTooLongException">
        ///     The specified path, file name, or both exceed the system-defined maximum length.
        ///     For example, on Windows-based platforms, paths must be less than 248 characters, and file names must be less than
        ///     260 characters.
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="mode">mode</paramref> contains an invalid value.</exception>
        public static CsvToAvroGenericWriter CreateFromPath(string schemaFilePath, string outputFilePath,
            Encoding encoding, Mode mode = Mode.Overwrite)
        {
            if (schemaFilePath == null)
            {
                throw new ArgumentNullException(nameof(schemaFilePath));
            }

            if (outputFilePath == null)
            {
                throw new ArgumentNullException(nameof(outputFilePath));
            }

            if (string.IsNullOrWhiteSpace(schemaFilePath))
            {
                throw new ArgumentException("Value cannot be an empty string, or contain only whitespace.", nameof(schemaFilePath));
            }

            if (string.IsNullOrWhiteSpace(outputFilePath))
            {
                throw new ArgumentException("Value cannot be an empty string, or contain only whitespace.", nameof(outputFilePath));
            }

            string jsonSchema = File.ReadAllText(schemaFilePath, encoding);
            var schema = (RecordSchema) Schema.Parse(jsonSchema);

            return new CsvToAvroGenericWriter(schema, outputFilePath, mode);
        }

        /// <summary>
        ///     Gets an instance of CsvToAvroGenericWriter.
        /// </summary>
        /// <param name="jsonSchema">A string containing the schema as properly formatted JSON.</param>
        /// <param name="outputFilePath">The path the Avro file should be written to.</param>
        /// <param name="mode">
        ///     If the output Avro file already exists, specified whether it should be overwritten or appended to.
        ///     The default is Overwrite.
        /// </param>
        /// <returns>A CsvToAvroGenericWriter object.</returns>
        /// <exception cref="ArgumentException">
        ///     <paramref name="jsonSchema">jsonSchema</paramref> is an empty string (""), or
        ///     contains only white space.
        /// </exception>
        /// <exception cref="ArgumentNullException"><paramref name="jsonSchema">jsonSchema</paramref> is null.</exception>
        /// <exception cref="SchemaParseException">
        ///     <paramref name="jsonSchema">jsonSchema</paramref> could not correctly converted
        ///     into valid JSON.
        /// </exception>
        /// <exception cref="ArgumentException">
        ///     <paramref name="outputFilePath">outputFilePath</paramref> is an empty string (""),
        ///     contains only white space, or contains one or more invalid characters, or refers to a non-file device, such as
        ///     "con:", "com1:", "lpt1:", etc. in an NTFS environment.
        /// </exception>
        /// <exception cref="NotSupportedException">
        ///     <paramref name="outputFilePath">outputFilePath</paramref> refers to a non-file
        ///     device, such as "con:", "com1:", "lpt1:", etc. in a non-NTFS environment.
        /// </exception>
        /// <exception cref="ArgumentNullException"><paramref name="outputFilePath">outputFilePath</paramref> is null.</exception>
        /// <exception cref="System.Security.SecurityException">The caller does not have the required permission.</exception>
        /// <exception cref="IOException">An I/O error, usually meaning the underlying stream has been unexpectedly closed.</exception>
        /// <exception cref="DirectoryNotFoundException">The specified path is invalid, such as being on an unmapped drive.</exception>
        /// <exception cref="PathTooLongException">
        ///     The specified path, file name, or both exceed the system-defined maximum length.
        ///     For example, on Windows-based platforms, paths must be less than 248 characters, and file names must be less than
        ///     260 characters.
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="mode">mode</paramref> contains an invalid value.</exception>
        public static CsvToAvroGenericWriter CreateFromJson(string jsonSchema, string outputFilePath,
            Mode mode = Mode.Overwrite)
        {
            if (jsonSchema == null)
            {
                throw new ArgumentNullException(nameof(jsonSchema));
            }

            if (string.IsNullOrWhiteSpace(jsonSchema))
            {
                throw new ArgumentException($"{nameof(jsonSchema)} is empty or contains only whitespace.",
                    nameof(jsonSchema));
            }

            var schema = (RecordSchema) Schema.Parse(jsonSchema);

            return new CsvToAvroGenericWriter(schema, outputFilePath, mode);
        }

        /// <summary>
        ///     Gets an instance of CsvToAvroGenericWriter.
        /// </summary>
        /// <param name="schema">An Avro RecordSchema object containing the schema.</param>
        /// <param name="outputFilePath">The path the Avro file should be written to.</param>
        /// <param name="mode">
        ///     If the output Avro file already exists, specified whether it should be overwritten or appended to.
        ///     The default is Overwrite.
        /// </param>
        /// <returns>A CsvToAvroGenericWriter object.</returns>
        /// <exception cref="ArgumentException">
        ///     <paramref name="outputFilePath">outputFilePath</paramref> is an empty string (""),
        ///     contains only white space, or contains one or more invalid characters; or refers to a non-file device, such as
        ///     "con:", "com1:", "lpt1:", etc. in an NTFS environment.
        /// </exception>
        /// <exception cref="NotSupportedException">
        ///     <paramref name="outputFilePath">outputFilePath</paramref> refers to a non-file
        ///     device, such as "con:", "com1:", "lpt1:", etc. in a non-NTFS environment.
        /// </exception>
        /// <exception cref="ArgumentNullException"><paramref name="outputFilePath">outputFilePath</paramref> is null.</exception>
        /// <exception cref="System.Security.SecurityException">The caller does not have the required permission.</exception>
        /// <exception cref="IOException">An I/O error, usually meaning the underlying stream has been unexpectedly closed.</exception>
        /// <exception cref="DirectoryNotFoundException">The specified path is invalid, such as being on an unmapped drive.</exception>
        /// <exception cref="PathTooLongException">
        ///     The specified path, file name, or both exceed the system-defined maximum length.
        ///     For example, on Windows-based platforms, paths must be less than 248 characters, and file names must be less than
        ///     260 characters.
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="mode">mode</paramref> contains an invalid value.</exception>
        public static CsvToAvroGenericWriter CreateFromSchema(RecordSchema schema, string outputFilePath,
            Mode mode = Mode.Overwrite)
        {
            return new CsvToAvroGenericWriter(schema, outputFilePath, mode);
        }

        /// <summary>
        ///     Sets the list of CSV headers.
        /// </summary>
        /// <param name="headerFields">An array containing the field names in the order in which the fields appear in the CSV data.</param>
        /// <exception cref="ArgumentException"><paramref name="headerFields">headerFields</paramref> contains no elements.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="headerFields">headerFields</paramref> is null.</exception>
        public void SetCsvHeader(string[] headerFields)
        {
            if (headerFields == null)
            {
                throw new ArgumentNullException(nameof(headerFields));
            }

            if (headerFields.Length == 0)
            {
                throw new ArgumentException($"{nameof(headerFields)} has no elements.", nameof(headerFields));
            }

            _csvHeaderFields = headerFields;
        }

        /// <summary>
        ///     Sets the list of CSV headers.
        /// </summary>
        /// <param name="header">
        ///     An separated string containing the field names in the order in which the fields appear in the CSV
        ///     data.
        /// </param>
        /// <param name="separator">The separator used in the supplied string. The default is comma (',').</param>
        /// <exception cref="ArgumentException">
        ///     <paramref name="header">header</paramref> is an empty string (""), or contains only
        ///     whitespace.
        /// </exception>
        /// <exception cref="ArgumentNullException"><paramref name="header">header</paramref> is null.</exception>
        public void SetCsvHeader(string header, char separator = DEFAULT_SEPARATOR)
        {
            if (header == null)
            {
                throw new ArgumentNullException(nameof(header));
            }

            if (string.IsNullOrWhiteSpace(header))
            {
                throw new ArgumentException($"{nameof(header)} is empty or contains only whitespace.", nameof(header));
            }

            _csvHeaderFields = header.Split(separator);
        }

        /// <summary>
        ///     Converts a CSV file to a file in Avro format, using the schema, output path, and mode specified by the writer.
        ///     NOTE: This method closes the writer once execution is complete.
        /// </summary>
        /// <param name="csvFilePath">
        ///     The path to a text file containing the CSV data. This overload assumes UTF8 encoding for the
        ///     file.
        /// </param>
        /// <param name="headerLinesToSkip">The number of lines to skip from the beginning of the CSV file. The default is 0.</param>
        /// <param name="separator">The separator used by the supplied CSV data. The default is comma (',').</param>
        /// <returns>The number of lines processed from the supplied file.</returns>
        /// <exception cref="ArgumentException">
        ///     <paramref name="csvFilePath">csvFilePath</paramref> is an empty string (""), or
        ///     contains only whitespace.
        /// </exception>
        /// <exception cref="ArgumentNullException"><paramref name="csvFilePath">csvFilePath</paramref> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        ///     <paramref name="headerLinesToSkip">headerLinesToSkip</paramref> is less
        ///     than zero.
        /// </exception>
        public int ConvertFromCsv(string csvFilePath, int headerLinesToSkip = 0, char separator = DEFAULT_SEPARATOR)
        {
            return ConvertFromCsv(csvFilePath, DEFAULT_ENCODING, headerLinesToSkip, separator);
        }

        /// <summary>
        ///     Converts a CSV file to a file in Avro format, using the schema, output path, and mode specified by the writer.
        ///     NOTE: This method closes the writer once execution is complete.
        /// </summary>
        /// <param name="csvFilePath">The path to a text file containing the CSV data.</param>
        /// <param name="encoding">The encoding used for the file containing the CSV data.</param>
        /// <param name="headerLinesToSkip">The number of lines to skip from the beginning of the CSV file. The default is 0.</param>
        /// <param name="separator">The separator used by the supplied CSV data. The default is comma (',').</param>
        /// <returns>The number of lines processed from the supplied file.</returns>
        /// <exception cref="ArgumentException">
        ///     <paramref name="csvFilePath">csvFilePath</paramref> is an empty string (""), or
        ///     contains only whitespace.
        /// </exception>
        /// <exception cref="ArgumentNullException"><paramref name="csvFilePath">csvFilePath</paramref> is null.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="encoding">encoding</paramref> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        ///     <paramref name="headerLinesToSkip">headerLinesToSkip</paramref> is less
        ///     than zero.
        /// </exception>
        public int ConvertFromCsv(string csvFilePath, Encoding encoding, int headerLinesToSkip = 0,
            char separator = DEFAULT_SEPARATOR)
        {
            if (csvFilePath == null)
            {
                throw new ArgumentNullException(nameof(csvFilePath));
            }

            if (encoding == null)
            {
                throw new ArgumentNullException(nameof(encoding));
            }

            if (string.IsNullOrWhiteSpace(csvFilePath))
            {
                throw new ArgumentException($"{nameof(csvFilePath)} is empty or contains only whitespace.",
                    nameof(csvFilePath));
            }

            if (headerLinesToSkip <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(headerLinesToSkip),
                    $"{nameof(headerLinesToSkip)} must be greater than zero.");
            }

            return ConvertFromCsv(new CsvTextFieldParser(csvFilePath, encoding), headerLinesToSkip, separator);
        }

        /// <summary>
        ///     Converts a CSV file to a file in Avro format, using the schema, output path, and mode specified by the writer.
        ///     NOTE: This method closes the writer once execution is complete.
        /// </summary>
        /// <param name="stream">A stream containing the CSV data. This overload assumes UTF8 encoding for the stream.</param>
        /// <param name="headerLinesToSkip">The number of lines to skip from the beginning of the CSV file. The default is 0.</param>
        /// <param name="separator">The separator used by the supplied CSV data. The default is comma (',').</param>
        /// <returns>The number of lines processed from the supplied file.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="stream">stream</paramref> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        ///     <paramref name="headerLinesToSkip">headerLinesToSkip</paramref> is less
        ///     than zero.
        /// </exception>
        public int ConvertFromCsv(Stream stream, int headerLinesToSkip = 0, char separator = DEFAULT_SEPARATOR)
        {
            return ConvertFromCsv(stream, DEFAULT_ENCODING, headerLinesToSkip, separator);
        }

        /// <summary>
        ///     Converts a CSV file to a file in Avro format, using the schema, output path, and mode specified by the writer.
        ///     NOTE: This method closes the writer once execution is complete.
        /// </summary>
        /// <param name="stream">A stream containing the CSV data.</param>
        /// <param name="encoding">The encoding used for the stream containing the CSV data.</param>
        /// <param name="headerLinesToSkip">The number of lines to skip from the beginning of the CSV file. The default is 0.</param>
        /// <param name="separator">The separator used by the supplied CSV data. The default is comma (',').</param>
        /// <returns>The number of lines processed from the supplied file.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="stream">stream</paramref> is null.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="encoding">encoding</paramref> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        ///     <paramref name="headerLinesToSkip">headerLinesToSkip</paramref> is less
        ///     than zero.
        /// </exception>
        public int ConvertFromCsv(Stream stream, Encoding encoding, int headerLinesToSkip = 0,
            char separator = DEFAULT_SEPARATOR)
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            if (encoding == null)
            {
                throw new ArgumentNullException(nameof(encoding));
            }

            if (headerLinesToSkip <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(headerLinesToSkip),
                    $"{nameof(headerLinesToSkip)} must be greater than zero.");
            }

            return ConvertFromCsv(new CsvTextFieldParser(stream, encoding), headerLinesToSkip, separator);
        }

        /// <summary>
        ///     Converts a CSV file to a file in Avro format, using the schema, output path, and mode specified by the writer.
        ///     NOTE: This method closes the writer once execution is complete.
        /// </summary>
        /// <param name="reader">A TextReader containing the CSV data.</param>
        /// <param name="headerLinesToSkip">The number of lines to skip from the beginning of the CSV file.</param>
        /// <param name="separator">The separator used by the supplied CSV data.</param>
        /// <returns>The number of lines processed from the supplied TextReader.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="reader">reader</paramref> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        ///     <paramref name="headerLinesToSkip">headerLinesToSkip</paramref> is less
        ///     than zero.
        /// </exception>
        public int ConvertFromCsv(TextReader reader, int headerLinesToSkip = 0, char separator = DEFAULT_SEPARATOR)
        {
            if (reader == null)
            {
                throw new ArgumentNullException(nameof(reader));
            }

            if (headerLinesToSkip <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(headerLinesToSkip),
                    $"{nameof(headerLinesToSkip)} must be greater than zero.");
            }

            return ConvertFromCsv(new CsvTextFieldParser(reader), headerLinesToSkip, separator);
        }

        private int ConvertFromCsv(CsvTextFieldParser parser, int headerLinesToSkip, char separator)
        {
            var counter = 0;

            using (parser)
            {
                parser.SetDelimiter(separator);
                parser.TrimWhiteSpace = false;

                for (var i = 0; i < headerLinesToSkip; i++)
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

            Dispose();

            return counter;
        }

        /// <summary>
        ///     Appends data to the end of the Avro file currently being written to.
        /// </summary>
        /// <param name="fields">An array of strings containing the data to be appended.</param>
        /// <exception cref="ArgumentNullException"><paramref name="fields">fields</paramref> is null.</exception>
        /// <exception cref="ArgumentException"><paramref name="fields">fields</paramref> contains no elements.</exception>
        public void Append(string[] fields)
        {
            if (fields == null)
            {
                throw new ArgumentNullException(nameof(fields));
            }

            if (fields.Length == 0)
            {
                throw new ArgumentException($"{nameof(fields)} has no elements.", nameof(fields));
            }

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
        ///     Appends data to the end of the Avro file currently being written to.
        /// </summary>
        /// <param name="line">
        ///     An separated string containing the data to be appended.
        ///     No attempt is made to perform any complex parsing; it simply splits the string based on the supplied separator.
        ///     If your line data has quoted values, newlines, escaped quotes, etc. you should use the other Append method.
        /// </param>
        /// <param name="separator">The separator used in the supplied string. The default is comma (',').</param>
        /// <exception cref="ArgumentException">
        ///     <paramref name="line">line</paramref> is an empty string (""), or contains only
        ///     whitespace.
        /// </exception>
        /// <exception cref="ArgumentNullException"><paramref name="line">line</paramref> is null.</exception>
        public void Append(string line, char separator = DEFAULT_SEPARATOR)
        {
            if (line == null)
            {
                throw new ArgumentNullException(nameof(line));
            }

            if (string.IsNullOrWhiteSpace(line))
            {
                throw new ArgumentException($"{nameof(line)} is empty, or contains only whitespace.", nameof(line));
            }

            Append(line.Split(separator));
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

        /// <summary>
        ///     Writes the Avro header metadata, closes the filestream, and cleans up resources.
        /// </summary>
        [Obsolete("The CloseWriter() method is deprecated. Please use the Dispose() method instead.", false)]
        public void CloseWriter()
        {
            _dataFileWriter.Close();
        }

        /// <summary>
        ///     Writes the Avro header metadata, closes the filestream, and cleans up resources.
        /// </summary>
        public void Dispose()
        {
            _dataFileWriter.Dispose();
        }
    }
}