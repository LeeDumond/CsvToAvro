using System;
using System.Collections.Generic;
using System.IO.Abstractions;
using System.IO.Abstractions.TestingHelpers;
using Xunit;

namespace CsvToAvro.Tests
{
    public class GenericWriterTests : IDisposable
    {
        public GenericWriterTests()
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
        public void CanCreateWriterFromPath()
        {
            _writer = CsvToAvroGenericWriter.CreateFromPath(@"C:\mySchema.avsc", @"C:\test\myAvro.avro");

            Assert.NotNull(_writer);
        }
    }
}