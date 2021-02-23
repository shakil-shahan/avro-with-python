import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

# import schema

# from file
# schema = avro.schema.parse(open("schema.avsc", "rb").read())

# define here
schema = avro.schema.parse("""{
    "namespace": "example.avro",
    "type": "record",
    "name": "User",
    "fields": [
            {"name": "name", "type": "string"},
            {"name": "favorite_number", "type": ["int", "null"]},
            {"name": "favorite_color", "type": ["string", "null"]}
    ]
}""")

# write a file
writer = DataFileWriter(
    open("./data/results.avro", "wb"), DatumWriter(), schema)
writer.append({"name": "Alyssa", "favorite_number": 256})
writer.append({"name": "Ben", "favorite_number": 7, "favorite_color": "red"})
writer.close()

# read a file
reader = DataFileReader(open("./data/results.avro", "rb"), DatumReader())
for data in reader:
    print(data)
reader.close()
