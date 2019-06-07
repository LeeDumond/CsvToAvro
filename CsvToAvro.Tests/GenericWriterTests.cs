using System;
using System.Collections.Generic;
using System.IO.Abstractions.TestingHelpers;
using System.Security.AccessControl;
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
            Assert.Equal("Value cannot be an empty string, or contain only whitespace.\r\nParameter name: schemaFilePath", exception.Message);
        }

        [Fact]
        public void CreateFromPath_WhitespaceSchemaPath_Throws()
        {
            Action action = () => CsvToAvroGenericWriter.CreateFromPath("   ", @"C:\test\myAvro.avro");

            var exception = Assert.Throws<ArgumentException>(action);
            Assert.Equal("Value cannot be an empty string, or contain only whitespace.\r\nParameter name: schemaFilePath", exception.Message);
        }

        [Fact]
        public void CreateFromPath_NullSchemaPath_Throws()
        {
            Action action = () => CsvToAvroGenericWriter.CreateFromPath(null, @"C:\test\myAvro.avro");

            var exception = Assert.Throws<ArgumentNullException>(action);
            Assert.Equal("Value cannot be null.\r\nParameter name: schemaFilePath", exception.Message);
        }

        [Fact]
        public void CreateFromPath_EmptyOutputPath_Throws()
        {
            CsvToAvroGenericWriter.FileSystemAbstract = _fileSystemFixture.MockFileSystem;
            Action action = () => CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", "");

            var exception = Assert.Throws<ArgumentException>(action);
            Assert.Equal("Value cannot be an empty string, or contain only whitespace.\r\nParameter name: outputFilePath", exception.Message);
        }

        [Fact]
        public void CreateFromPath_WhitespaceOutputPath_Throws()
        {
            CsvToAvroGenericWriter.FileSystemAbstract = _fileSystemFixture.MockFileSystem;
            Action action = () => CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", "   ");

            var exception = Assert.Throws<ArgumentException>(action);
            Assert.Equal("Value cannot be an empty string, or contain only whitespace.\r\nParameter name: outputFilePath", exception.Message);
        }

        [Fact]
        public void CreateFromPath_NullOutputPath_Throws()
        {
            CsvToAvroGenericWriter.FileSystemAbstract = _fileSystemFixture.MockFileSystem;
            Action action = () => CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", null);

            var exception = Assert.Throws<ArgumentNullException>(action);
            Assert.Equal("Value cannot be null.\r\nParameter name: outputFilePath", exception.Message);
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
    }
}
