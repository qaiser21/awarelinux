from pyspark.sql import SparkSession
from flask import Flask, request
import yaml

spark = SparkSession.builder.getOrCreate()

app = Flask(__name__)


@app.route('/', methods=['GET'])
def return_data():
    config_data = open('config.yaml', 'r')
    dct_yaml_data = yaml.load(config_data)
    dct_input_param = request.args.to_dict()
    sql_query = dct_yaml_data[dct_input_param['kpi']]
    df = spark.sql(sql_query)
    results = df.select("*").toPandas().to_json(orient='records')
    return results

