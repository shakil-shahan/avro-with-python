import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

# Define schema
schema = avro.schema.parse("""{
    "type": "record",
    "name": "BD23",
    "namespace": "senscom.avro",
    "fields": [
        { "name": "_id", "type": "string" },
        { "name": "type", "type": "string" },
        { "name": "sensor_id", "type": "string" },
        { "name": "rssi", "type": "double" },
        { "name": "timestamp", "type": "double" },
        {
            "name": "jsondata",
            "type": {
                "type": "record",
                "name": "jsondata",
                "fields": [
                    { "name": "Humid1", "type": "double" },
                    { "name": "Humid2", "type": "double" },
                    { "name": "Humid3", "type": "double" },
                    { "name": "Humid4", "type": "double" },
                    { "name": "Temp1", "type": "double" },
                    { "name": "Temp2", "type": "double" },
                    { "name": "Temp3", "type": "double" },
                    { "name": "Temp4", "type": "double" },
                    { "name": "Status", "type": "double" }
                ]
            }
        },
        { "name": "gatewayid", "type": "double" },
        { "name": "createdAt", "type": "string" }
    ]
}""")


# write a file
writer = DataFileWriter(
    open("./avroData/result.avro", "wb"), DatumWriter(), schema)

# Data append
writer.append({
    "_id": "3806166:5",
    "type": "log",
    "sensor_id": "0x5051a9746f52",
    "rssi": -79,
    "timestamp": 1614084286,
    "jsondata": {
        "Humid1": 119,
        "Humid2": 27.5,
        "Humid3": 119,
        "Humid4": 119,
        "Temp1": 128.9,
        "Temp2": 21.5,
        "Temp3": 128.9,
        "Temp4": 128.9,
        "Status": 49649
    },
    "gatewayid": 863859041004586,
    "createdAt": "2021-02-23T11:58:59.425Z"
})
writer.close()

# read a file
reader = DataFileReader(open("./avroData/result.avro", "rb"), DatumReader())
for data in reader:
    print(data)
reader.close()
