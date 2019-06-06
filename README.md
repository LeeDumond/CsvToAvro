# CsvToAvro

![Nuget](https://img.shields.io/nuget/v/CsvToAvro.svg)

A simple .NET Standard 2.0 library to convert CSV files to AVRO files, when accompanied by a known schema that describes the field names and data types in the source CSV file.

## How it works

The major player here is the `CsvToAvro` class library. The `CsvToAvroConsoleApp` class is included as a demonstration of usage.

To use, import the `CsvToAvro` library, then use the static `CsvToAvroGenericWriter.CreateFrom...` factory methods to get a writer object. Each of them accepts as arguments a schema, the file path for the resulting AVRO file, and a mode that indicates whether you want to *overwrite* or *append* to the output file if one already exists.

The method to use will depend on how your schema is represented:

- `CsvToAvroGenericWriter.CreateFromPath()` if you have a file path to a text file containing the schema in JSON format.
- `CsvToAvroGenericWriter.CreateFromJson()` if you only have the raw text containing the schema in JSON format. This is handy in case you don't have file access to the schema.
- `CsvToAvroGenericWriter.CreateFromSchema()` if you already have a pre-defined `RecordSchema` object containing the schema.

[Read the Avro schema specification here.](https://avro.apache.org/docs/1.8.2/spec.html#schemas)

### Basic Usage

Once you have a writer, you can just tell it where your CSV file lives, and let it do all of the work:

```C#
string schemaFilePath = @"C:\CsvToAvro\mySchema.avsc";
string csvFilePath = @"C:\CsvToAvro\myCsv.csv";
string outputFilePath = @"C:\CsvToAvro\myAvro.avro";
            
CsvToAvroGenericWriter writer = CsvToAvroGenericWriter.CreateFromPath(schemaFilePath, outputFilePath);
int counter = writer.ConvertFromCsv(csvFilePath);

Console.WriteLine($"There were {counter} lines processed from: {csvFilePath}");
```
In case you don't have file access to the CSV data, you can supply the raw CSV data via a TextReader:

```C#
using(TextReader reader = new StringReader(myCsvString))
{
    writer.ConvertFromCsv(reader);
}
```  

You may also indicate how many header lines should be skipped in the CSV file (*default is 0*), and a custom separator (*default is comma*).

This usage is capable of parsing complex CSV data including quoted lines, embedded newlines, escaped quotes, and the like; and is recommended for most scenarios.

### Advanced Usage

If you need more control, you may loop through your CSV data yourself and use any of the `Append()` overloads to add the values from each line to the writer manually (don't forget to close the writer after you're done!):

```C#
int counter = 0;
using (var reader = new StreamReader(csvFilePath))
{
    string line;
    while ((line = reader.ReadLine()) != null)
    {
        if (!string.IsNullOrWhiteSpace(line))
        {
            writer.Append(line);
            counter++;
        }
    }
}

writer.CloseWriter();

Console.WriteLine($"There were {counter} lines processed from: {csvFilePath}");
```
The `Append()` methods accept either an array of string values, or a raw separated string. 

Note that the `Append(string line, char separator = DEFAULT_SEPARATOR)` overload makes no attempt to perform any complex parsing; it simply splits the string based on the supplied separator (the default is comma). If your line data has quoted values, newlines, escaped quotes, etc. you should parse the data yourself and use the `Append(string[] fields)` overload, or consider using the `ConvertFromCsv` method as explained above.

### Dealing with Field Ordering

Normally, the writer expects the values in the CSV file to appear in the same order they are defined in the schema. However, this may not always be the case. In those situations, you can tell the writer in what order the fields actually appear in your data:

```C#
string[] headerFields = { "name", "address", "city", "state", "zip", "email" };
writer.SetCsvHeader(headerFields);
``` 

## Contributions

Issues and contributions are most welcome. *Please target all pull requests to the* `development` *branch only*.
