using System;
using System.Collections.Generic;
using System.IO.Abstractions;
using System.IO.Abstractions.TestingHelpers;
using Xunit;

namespace CsvToAvro.Tests
{
    public class GenericWriterExceptionTests : IDisposable
    {
        public GenericWriterExceptionTests()
        {
            CsvToAvroGenericWriter.FileSystemAbstract = GetMockFileSystem();
        }

        public void Dispose()
        {
            _writer?.Dispose();
        }

        private CsvToAvroGenericWriter _writer;

        private IFileSystem GetMockFileSystem()
        {
            var jsonSchema = @"{
   ""type"" : ""record"",
   ""namespace"" : ""dumond.lee"",
   ""name"" : ""Employee"",
   ""fields"" : [
      { ""name"" : ""Name"" , ""type"" : ""string"" },
      { ""name"" : ""Age"" , ""type"" : ""int"" }
   ]
}";

            return new MockFileSystem(new Dictionary<string, MockFileData>
            {
                {@"C:\mySchema.avsc", new MockFileData(jsonSchema)}, {@"C:\test", new MockDirectoryData()}
            });
        }

        [Fact]
        public void CreateFromPath_EmptyOutputPath_Throws()
        {
            Action action = () => _writer = CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", "");

            Assert.ThrowsAny<Exception>(action);
        }

        [Fact]
        public void CreateFromPath_EmptySchemaPath_Throws()
        {
            Action action = () => _writer = CsvToAvroGenericWriter.CreateFromPath("", @"C:\test\myAvro.avro");

            Assert.ThrowsAny<Exception>(action);
        }

        [Fact]
        public void CreateFromPath_InvalidMode_Throws()
        {
            Action action = () => _writer = CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc",
                @"C:\test\myAvro.avro", (CsvToAvroGenericWriter.Mode) 2);

            Assert.ThrowsAny<Exception>(action);
        }

        [Fact]
        public void CreateFromPath_NullEncoding_Throws()
        {
            Action action = () =>
                _writer = CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", @"C:\test\myAvro.avro", null);

            Assert.ThrowsAny<Exception>(action);
        }

        [Fact]
        public void CreateFromPath_NullOutputPath_Throws()
        {
            Action action = () => _writer = CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", null);

            Assert.ThrowsAny<Exception>(action);
        }

        [Fact]
        public void CreateFromPath_NullSchemaPath_Throws()
        {
            Action action = () => _writer = CsvToAvroGenericWriter.CreateFromPath(null, @"C:\test\myAvro.avro");

            Assert.ThrowsAny<Exception>(action);
        }

        [Fact]
        private void CreateFromPath_OutputDirectoryNotFound_Throws()
        {
            Action action = () =>
                _writer = CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", @"C:\test123\myAvro.avro");

            Assert.ThrowsAny<Exception>(action);
        }

        [Fact]
        private void CreateFromPath_OutputPathContainsInvalidCharacters_Throws()
        {
            Action action = () =>
                _writer = CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", @"C:\te|st\myAvro.avro");

            Assert.ThrowsAny<Exception>(action);
        }

        [Fact]
        private void CreateFromPath_SchemaDirectoryNotFound_Throws()
        {
            Action action = () =>
                _writer = CsvToAvroGenericWriter.CreateFromPath(@"C:\test123\mySchema.avsc", @"C:\test\myAvro.avro");

            Assert.ThrowsAny<Exception>(action);
        }

        [Fact]
        private void CreateFromPath_SchemaFileContainsInvalidJson_Throws()
        {
            var jsonSchema = "{I'm not valid JSON!}";

            var fileSystem = new MockFileSystem(new Dictionary<string, MockFileData>
            {
                {@"C:\mySchema.avsc", new MockFileData(jsonSchema)}, {@"C:\test", new MockDirectoryData()}
            });

            CsvToAvroGenericWriter.FileSystemAbstract = fileSystem;
            Action action = () =>
                _writer = CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", @"C:\test\myAvro.avro");

            Assert.ThrowsAny<Exception>(action);
        }

        [Fact]
        private void CreateFromPath_SchemaFileIsEmpty_Throws()
        {
            var jsonSchema = "";

            var fileSystem = new MockFileSystem(new Dictionary<string, MockFileData>
            {
                {@"C:\mySchema.avsc", new MockFileData(jsonSchema)}, {@"C:\test", new MockDirectoryData()}
            });

            CsvToAvroGenericWriter.FileSystemAbstract = fileSystem;
            Action action = () =>
                _writer = CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", @"C:\test\myAvro.avro");

            Assert.ThrowsAny<Exception>(action);
        }

        [Fact]
        private void CreateFromPath_SchemaFileIsWhitespace_Throws()
        {
            var jsonSchema = "   ";

            var fileSystem = new MockFileSystem(new Dictionary<string, MockFileData>
            {
                {@"C:\mySchema.avsc", new MockFileData(jsonSchema)}, {@"C:\test", new MockDirectoryData()}
            });

            CsvToAvroGenericWriter.FileSystemAbstract = fileSystem;
            Action action = () =>
                _writer = CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", @"C:\test\myAvro.avro");

            Assert.ThrowsAny<Exception>(action);
        }

        [Fact]
        private void CreateFromPath_SchemaFileNotFound_Throws()
        {
            Action action = () =>
                _writer = CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema123.avsc", @"C:\test\myAvro.avro");

            Assert.ThrowsAny<Exception>(action);
        }

        [Fact]
        private void CreateFromPath_SchemaPathContainsInvalidCharacters_Throws()
        {
            Action action = () =>
                _writer = CsvToAvroGenericWriter.CreateFromPath(@"C:\badPath\my|Schema.avsc", @"C:\test\myAvro.avro");

            Assert.ThrowsAny<Exception>(action);
        }

        [Fact]
        public void CreateFromPath_WhitespaceOutputPath_Throws()
        {
            Action action = () => _writer = CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", "   ");

            Assert.ThrowsAny<Exception>(action);
        }

        [Fact]
        public void CreateFromPath_WhitespaceSchemaPath_Throws()
        {
            Action action = () => _writer = CsvToAvroGenericWriter.CreateFromPath("   ", @"C:\test\myAvro.avro");

            Assert.ThrowsAny<Exception>(action);
        }

        [Fact]
        public void SetCsvHeader_NullFieldsCollection_Throws()
        {
            _writer = CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", @"C:\test\myAvro.avro");
            Action action = () => _writer.SetCsvHeader(null);

            Assert.Throws<ArgumentNullException>(action);
        }

        [Fact]
        public void SetCsvHeader_EmptyFieldsCollection_Throws()
        {
            _writer = CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", @"C:\test\myAvro.avro");
            Action action = () => _writer.SetCsvHeader(new string[0]);

            Assert.Throws<ArgumentException>(action);
        }

        [Fact]
        public void SetCsvHeader_NullHeader_Throws()
        {
            _writer = CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", @"C:\test\myAvro.avro");
            Action action = () => _writer.SetCsvHeader((string) null);

            Assert.Throws<ArgumentNullException>(action);
        }

        [Fact]
        public void SetCsvHeader_EmptyHeader_Throws()
        {
            _writer = CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", @"C:\test\myAvro.avro");
            Action action = () => _writer.SetCsvHeader("");

            Assert.Throws<ArgumentException>(action);
        }

        [Fact]
        public void SetCsvHeader_WhitespaceHeader_Throws()
        {
            _writer = CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", @"C:\test\myAvro.avro");
            Action action = () => _writer.SetCsvHeader("  \n ");

            Assert.Throws<ArgumentException>(action);
        }

        [Fact]
        public void ReadAllTextTest()
        {
            _writer = CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", @"C:\test\myAvro.avro");

            Assert.NotNull(_writer);
        }
    }
}