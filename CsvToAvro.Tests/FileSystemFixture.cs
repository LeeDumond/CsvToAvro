using System.Collections.Generic;
using System.IO.Abstractions.TestingHelpers;

namespace CsvToAvro.Tests
{
    public class FileSystemFixture
    {
        public FileSystemFixture()
        {
            string jsonSchema = @"{
   ""type"" : ""record"",
   ""namespace"" : ""dumond.lee"",
   ""name"" : ""Employee"",
   ""fields"" : [
      { ""name"" : ""Name"" , ""type"" : ""string"" },
      { ""name"" : ""Age"" , ""type"" : ""int"" }
   ]
}";

            MockFileSystem = new MockFileSystem(new Dictionary<string, MockFileData>
            {
                {@"C:\mySchema.avsc", new MockFileData(jsonSchema)},
                {@"C:\test", new MockDirectoryData() }
            });
        }

        public MockFileSystem MockFileSystem { get; private set; }
    }
}