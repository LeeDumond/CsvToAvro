# CsvToAvro

A simple .NET Core library to convert CSV files to AVRO files, when accompanied by a known schema that describes the field names and data types in the source CSV file.

## How it works

The major player here is the `CsvToAvroGenericWriter.cs` class. The `Program.cs` class is included as a demonstration of usage.

Use the static `CsvToAvroGenericWriter.CreateFrom...` methods to get a writer object. Each of them accepts as arguments a schema, the file path for the resulting AVRO file, and a mode that indicates whether you want to *overwrite* or *append* to the output file if one already exists.

The factory method to use will depend on how your schema is represented:

- `CsvToAvroGenericWriter.CreateFromPath` if you have a file path to a text file containing the schema in JSON format.
- `CsvToAvroGenericWriter.CreateFromJson` if you only have the raw text containing the schema in JSON format.
- `CsvToAvroGenericWriter.CreateFromSchema` if you already have pre-defined `RecordSchema` object containing the schema.

### Basic Usage

Once you have a writer, you can just tell it where your CSV file lives let it do all of the work:

```C#
int counter = writer.ConvertFromCsv(csvFilePath);
Console.WriteLine($"There were {counter} lines processed from: {csvFilePath}");
```

You may also indicate if there are header lines that should be skipped (*default is 0*), and a custom separator (*default is comma*).

### Advanced Usage

If you need more control, you may parse your CSV yourself and use any of the `Append()` overloads to add the values from each line to the writer manually (don't forget to close the writer after you're done!):

```C#
using (var reader = new StreamReader(csvFilePath))
{
    string line;
    while ((line = reader.ReadLine()) != null)
    {
        if (!string.IsNullOrEmpty(line))
        {
            writer.Append(line);
        }
    }
}

writer.CloseWriter();

Console.WriteLine($"There were {counter} lines processed from: {csvFilePath}");
```
The `Append()` methods accept either an array of string values, or a raw separated string. 

Note that the `Append(string line, char separator = DEFAULT_SEPARATOR)` overload makes no attempt to perform any complex parsing; it simply splits the string based on the supplied separator (the default is comma). If your line data has quoted values, newlines, escaped quotes, etc. you should use the `Append(string[] fields)` overload.