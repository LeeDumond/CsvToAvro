using System.Collections.Generic;
using Microsoft.Analytics.Interfaces;
using Avro.File;
using Avro.Generic;
using System.IO;
using Avro;

namespace CsvToAvro
{
    public class AvroExtractor : IExtractor
    {
        private readonly string _avroSchema;

        public AvroExtractor(string avroSchema)
        {
            this._avroSchema = avroSchema;
        }

        public override IEnumerable<IRow> Extract(IUnstructuredReader input, IUpdatableRow output)
        {
            Schema avschema = Schema.Parse(_avroSchema);
            var reader = new GenericDatumReader<GenericRecord>(avschema, avschema);

            using (var ms = new MemoryStream())
            {
                CreateSeekableStream(input, ms);
                ms.Position = 0;

                IFileReader<GenericRecord> fileReader = DataFileReader<GenericRecord>.OpenReader(ms, avschema);

                while (fileReader.HasNext())
                {
                    GenericRecord avroRecord = fileReader.Next();

                    foreach (IColumn column in output.Schema)
                    {
                        if (avroRecord[column.Name] != null)
                        {
                            output.Set(column.Name, avroRecord[column.Name]);
                        }
                        else
                        {
                            output.Set<object>(column.Name, null);
                        }
                    }

                    yield return output.AsReadOnly();
                }
            }
        }

        private void CreateSeekableStream(IUnstructuredReader input, MemoryStream output)
        {
            input.BaseStream.CopyTo(output);
        }

    }
}