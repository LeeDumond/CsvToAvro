using System;
using Xunit;

namespace CsvToAvro.Tests
{
    public class GenericWriterTests
    {
        [Fact]
        public void CreateFromPath_EmptySchemaPath_Throws()
        {
            Action action = () => CsvToAvroGenericWriter.CreateFromPath("", @"C:\output");

            var exception = Assert.Throws<ArgumentException>(action);
            Assert.Equal("Value cannot be an empty string, or contain only whitespace.\r\nParameter name: schemaFilePath", exception.Message);
        }

        [Fact]
        public void CreateFromPath_WhitespaceSchemaPath_Throws()
        {
            Action action = () => CsvToAvroGenericWriter.CreateFromPath("   ", @"C:\output");

            var exception = Assert.Throws<ArgumentException>(action);
            Assert.Equal("Value cannot be an empty string, or contain only whitespace.\r\nParameter name: schemaFilePath", exception.Message);
        }

        [Fact]
        public void CreateFromPath_NullSchemaPath_Throws()
        {
            Action action = () => CsvToAvroGenericWriter.CreateFromPath(null, @"C:\output");

            var exception = Assert.Throws<ArgumentNullException>(action);
            Assert.Equal("Value cannot be null.\r\nParameter name: schemaFilePath", exception.Message);
        }

        [Fact]
        public void CreateFromPath_EmptyOutputPath_Throws()
        {
            Action action = () => CsvToAvroGenericWriter.CreateFromPath(@"C:\csvtoavro\myschema.avsc", "");

            var exception = Assert.Throws<ArgumentException>(action);
            Assert.Equal("Value cannot be an empty string, or contain only whitespace.\r\nParameter name: outputFilePath", exception.Message);
        }

        [Fact]
        public void CreateFromPath_WhitespaceOutputPath_Throws()
        {
            Action action = () => CsvToAvroGenericWriter.CreateFromPath(@"C:\csvtoavro\myschema.avsc", "   ");

            var exception = Assert.Throws<ArgumentException>(action);
            Assert.Equal("Value cannot be an empty string, or contain only whitespace.\r\nParameter name: outputFilePath", exception.Message);
        }

        [Fact]
        public void CreateFromPath_NullOutputPath_Throws()
        {
            Action action = () => CsvToAvroGenericWriter.CreateFromPath(@"C:\csvtoavro\myschema.avsc", null);

            var exception = Assert.Throws<ArgumentNullException>(action);
            Assert.Equal("Value cannot be null.\r\nParameter name: outputFilePath", exception.Message);
        }
    }
}
