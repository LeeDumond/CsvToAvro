using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.IO.Abstractions.TestingHelpers;
using System.Security.AccessControl;
using Avro;
using Moq;
using Newtonsoft.Json;
using Xunit;

namespace CsvToAvro.Tests
{
    public class GenericWriterTests : IClassFixture<FileSystemFixture>
    {
        private FileSystemFixture _fileSystemFixture;

        public GenericWriterTests(FileSystemFixture fileSystemFixture)
        {
            _fileSystemFixture = fileSystemFixture;
        }

        [Fact]
        public void CreateFromPath_EmptySchemaPath_Throws()
        {
            Action action = () => CsvToAvroGenericWriter.CreateFromPath("", @"C:\test\myAvro.avro");

            var exception = Assert.Throws<ArgumentException>(action);
            Assert.Equal(GetEmptyStringExceptionMessage("schemaFilePath"), exception.Message);
        }

        [Fact]
        public void CreateFromPath_WhitespaceSchemaPath_Throws()
        {
            Action action = () => CsvToAvroGenericWriter.CreateFromPath("   ", @"C:\test\myAvro.avro");

            var exception = Assert.Throws<ArgumentException>(action);
            Assert.Equal(GetEmptyStringExceptionMessage("schemaFilePath"), exception.Message);
        }

        [Fact]
        public void CreateFromPath_NullSchemaPath_Throws()
        {
            Action action = () => CsvToAvroGenericWriter.CreateFromPath(null, @"C:\test\myAvro.avro");

            var exception = Assert.Throws<ArgumentNullException>(action);
            Assert.Equal(GetNullExceptionMessage("schemaFilePath"), exception.Message);
        }

        [Fact]
        public void CreateFromPath_EmptyOutputPath_Throws()
        {
            CsvToAvroGenericWriter.FileSystemAbstract = _fileSystemFixture.MockFileSystem;
            Action action = () => CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", "");

            var exception = Assert.Throws<ArgumentException>(action);
            Assert.Equal(GetEmptyStringExceptionMessage("outputFilePath"), exception.Message);
        }

        [Fact]
        public void CreateFromPath_WhitespaceOutputPath_Throws()
        {
            CsvToAvroGenericWriter.FileSystemAbstract = _fileSystemFixture.MockFileSystem;
            Action action = () => CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", "   ");

            var exception = Assert.Throws<ArgumentException>(action);
            Assert.Equal(GetEmptyStringExceptionMessage("outputFilePath"), exception.Message);
        }

        [Fact]
        public void CreateFromPath_NullOutputPath_Throws()
        {
            CsvToAvroGenericWriter.FileSystemAbstract = _fileSystemFixture.MockFileSystem;
            Action action = () => CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", null);

            var exception = Assert.Throws<ArgumentNullException>(action);
            Assert.Equal(GetNullExceptionMessage("outputFilePath"), exception.Message);
        }

        [Fact]
        public void CreateFromPath_InvalidMode_Throws()
        {
            CsvToAvroGenericWriter.FileSystemAbstract = _fileSystemFixture.MockFileSystem;
            Action action = () => CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", @"C:\test\myAvro.avro", (CsvToAvroGenericWriter.Mode) 2);

            var exception = Assert.Throws<ArgumentOutOfRangeException>(action);
            Assert.Equal("Specified argument was out of the range of valid values.\r\nParameter name: mode", exception.Message);
        }

        [Fact]
        private void CreateFromPath_SchemaFileIsEmpty_Throws()
        {
            var fileSystem = new MockFileSystem(new Dictionary<string, MockFileData>
            {
                {@"C:\mySchema.avsc", new MockFileData("")},
                {@"C:\test", new MockDirectoryData() }
            });

            CsvToAvroGenericWriter.FileSystemAbstract = fileSystem;
            Action action = () => CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", @"C:\test\myAvro.avro");

            var exception = Assert.Throws<ArgumentException>(action);
            Assert.Equal(GetEmptyStringExceptionMessage("jsonSchema"), exception.Message);
        }

        [Fact]
        private void CreateFromPath_SchemaFileIsWhitespace_Throws()
        {
            var fileSystem = new MockFileSystem(new Dictionary<string, MockFileData>
            {
                {@"C:\mySchema.avsc", new MockFileData("   ")},
                {@"C:\test", new MockDirectoryData() }
            });

            CsvToAvroGenericWriter.FileSystemAbstract = fileSystem;
            Action action = () => CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", @"C:\test\myAvro.avro");

            var exception = Assert.Throws<ArgumentException>(action);
            Assert.Equal(GetEmptyStringExceptionMessage("jsonSchema"), exception.Message);
        }

        [Fact]
        private void CreateFromPath_SchemaFileContainsInvalidJson_Throws()
        {
            string jsonSchema = "{I'm not valid JSON!}";

            var fileSystem = new MockFileSystem(new Dictionary<string, MockFileData>
                        {
                            {@"C:\mySchema.avsc", new MockFileData(jsonSchema)},
                            {@"C:\test", new MockDirectoryData() }
                        });

            CsvToAvroGenericWriter.FileSystemAbstract = fileSystem;
            Action action = () => CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", @"C:\test\myAvro.avro");

            Assert.ThrowsAny<JsonException>(action);
        }

        [Fact]
        private void CreateFromPath_SchemaPathContainsInvalidCharacters_Throws()
        {
            CsvToAvroGenericWriter.FileSystemAbstract = null;
            Action action = () => CsvToAvroGenericWriter.CreateFromPath(@"C:\badPath\my|Schema.avsc", @"C:\test\myAvro.avro");

            var exception = Assert.Throws<ArgumentException>(action);
            Assert.Equal("The path or file name contains one or more invalid characters.\r\nParameter name: schemaFilePath", exception.Message);
            
        }

        [Fact]
        private void CreateFromPath_OutputPathContainsInvalidCharacters_Throws()
        {
            CsvToAvroGenericWriter.FileSystemAbstract = _fileSystemFixture.MockFileSystem;
            Action action = () => CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", @"C:\te|st\myAvro.avro");

            var exception = Assert.Throws<ArgumentException>(action);
            Assert.Equal("The path or file name contains one or more invalid characters.\r\nParameter name: outputFilePath", exception.Message);
        }

        [Fact]
        private void CreateFromPath_SchemaDirectoryNotFound_Throws()
        {
            CsvToAvroGenericWriter.FileSystemAbstract = new FileSystem();
            Action action = () => CsvToAvroGenericWriter.CreateFromPath(@"C:\test123\mySchema.avsc", @"C:\test\myAvro.avro");

            Assert.Throws<DirectoryNotFoundException>(action);
        }

        [Fact]
        private void CreateFromPath_SchemaFileNotFound_Throws()
        {
            CsvToAvroGenericWriter.FileSystemAbstract = new FileSystem();
            Action action = () => CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema123.avsc", @"C:\test\myAvro.avro");

            Assert.Throws<FileNotFoundException>(action);
        }

        [Fact]
        private void CreateFromPath_OutputDirectoryNotFound_Throws()
        {
            CsvToAvroGenericWriter.FileSystemAbstract = _fileSystemFixture.MockFileSystem;
            Action action = () => CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", @"C:\test123\myAvro.avro");

            Assert.Throws<DirectoryNotFoundException>(action);
        }

        [Fact]
        public void CreateFromPath_NullEncoding_Throws()
        {
            CsvToAvroGenericWriter.FileSystemAbstract = _fileSystemFixture.MockFileSystem;
            Action action = () =>
                CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", @"C:\test\myAvro.avro", null);

            var exception = Assert.Throws<ArgumentNullException>(action);
            Assert.Equal(GetNullExceptionMessage("encoding"), exception.Message);
        }

        [Fact]
        public void ReadAllTextTest()
        {
//            string jsonSchema = @"{
//   ""type"" : ""record"",
//   ""namespace"" : ""dumond.lee"",
//   ""name"" : ""Employee"",
//   ""fields"" : [
//      { ""name"" : ""Name"" , ""type"" : ""string"" },
//      { ""name"" : ""Age"" , ""type"" : ""int"" }
//   ]
//}";

//            var fileSystem = new MockFileSystem(new Dictionary<string, MockFileData>
//            {
//                {@"C:\mySchema.avsc", new MockFileData(jsonSchema)},
//                {@"C:\test", new MockDirectoryData() }
//            });

            CsvToAvroGenericWriter.FileSystemAbstract = _fileSystemFixture.MockFileSystem;
            var writer = CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", @"C:\test\myAvro.avro");
            

        }

        private string GetEmptyStringExceptionMessage(string paramName) => $"Value cannot be an empty string, or contain only whitespace.\r\nParameter name: {paramName}";

        private string GetNullExceptionMessage(string paramName) => $"Value cannot be null.\r\nParameter name: {paramName}";
    }
}
