import numpy as np
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.streaming.context import StreamingContext
from pyspark.streaming.dstream import DStream
from pyspark.ml.linalg import DenseVector
import os

from transforms import Transforms
from trainer import SparkConfig

import json

class DataLoader:
    def __init__(self, 
                 sparkContext:SparkContext, 
                 sparkStreamingContext: StreamingContext, 
                 sqlContext: SQLContext,
                 sparkConf: SparkConfig, 
                 transforms: Transforms) -> None:
        
        self.sc = sparkContext
        self.ssc = sparkStreamingContext
        self.sparkConf = sparkConf
        self.sql_context = sqlContext
        self.stream = self.ssc.socketTextStream(
            hostname=self.sparkConf.stream_host, 
            port=self.sparkConf.port
        )
        self.transforms = transforms

    def parse_stream(self) -> DStream:
        def safe_json_load(line):
            try:
                return json.loads(line)
            except json.JSONDecodeError as e:
                print(f"JSON decode error: {e}, line: {line}")
                return {}

        json_stream = self.stream.map(safe_json_load)        
        json_stream_exploded = json_stream.flatMap(lambda x: x.values() if x else [])
        # Debug: Print raw JSON data
        json_stream_exploded.foreachRDD(lambda rdd: print(f"Raw JSON data: {len(rdd.collect())}"))

        json_stream_exploded = json_stream_exploded.map(lambda x: list(x.values()))
        # Debug: Print after extracting values
        json_stream_exploded.foreachRDD(lambda rdd: print(f"Extracted values"))
    
        def safe_reshape(x):
            try:
                pixels = np.array(x[:-1]).reshape(3, 32, 32).transpose(1, 2, 0).astype(np.uint8)
                return [pixels, x[-1]]
            except Exception as e:
                print(f"Reshape error: {e}, data: {x}")
                return [np.zeros((32, 32, 3), dtype=np.uint8), x[-1]]

        pixels = json_stream_exploded.map(safe_reshape)
        # Debug: Print after reshaping
        pixels.foreachRDD(lambda rdd: print(f"Reshaped data"))
        pixels = DataLoader.preprocess(pixels, self.transforms)
        
        return pixels

    @staticmethod
    def preprocess(stream: DStream, transforms: Transforms) -> DStream:
        print('Preprocessing',stream.pprint())
        #stream = stream.map(lambda x: [transforms.transform(x[0]).reshape(32, 32, 3).reshape(-1).tolist(), x[1]])
        stream = stream.map(lambda x: [DenseVector(x[0]), x[1]])
        return stream