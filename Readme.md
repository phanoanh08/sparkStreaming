#SparkStreaming
1. Create database 
- example in demo/sql.sql
2. About model
- Training data: https://github.com/sonlam1102/vihsd
- Run model/buildModel.py to re-train model
3. Collect data
- collect.py to get source.json (example in demo/source.json)
- saveToBD.py
- partition.py 
4. Stream
- dataToStream to make a streaming data
- streamData.py to stream HSD by comments
