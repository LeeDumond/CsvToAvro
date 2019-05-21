# CsvToAvro

A simple library, written in C#, to convert CSV files to AVRO files, when accompanied by a known schema that describes the field names and data types in the source CSV file.

## How it works

The major player here is the `CsvToAvroGenericWriter.cs` class. The `Program.cs` class is included as a demonstration of usage.

Use the static `CsvToAvroGenericWriter.CreateFrom...` methods to get a writer object. Each of them accepts as arguments a schema, the file path for the resulting AVRO file, and a mode that indicates whether you want to *overwrite* or *append* to the output file if one already exists.

The factory method to use will depend on how your schema is represented:

- `CsvToAvroGenericWriter.CreateFromPath` if you have a file path to a text file containing the schema in JSON format.
- `CsvToAvroGenericWriter.CreateFromJson` if you only have the raw text containing the schema in JSON format.
- `CsvToAvroGenericWriter.CreateFromSchema` if you already have pre-defined `RecordSchema` object containing the schema.
