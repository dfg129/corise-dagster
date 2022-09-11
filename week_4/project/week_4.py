from typing import List

from project.resources import S3, Redis, redis_resource, s3_resource

from dagster import Nothing, asset, Out, repository, with_resources
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock
from datetime import datetime


@asset(
     required_resource_keys={"s3"},
)
def get_s3_data(context, group_name="corise"):
    output = list()
    stocks = context.resources.s3.get_data("key_name")
    for row in stocks:
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@asset()
def process_data(get_s3_data):
    high_val = 0
    date = datetime
    
    for stock in get_s3_data:
        if stock.high > high_val:
            high_val = stock.high
            date = stock.date
    
    return Aggregation(date=date, high=high_val)

@asset(
    required_resource_keys={"redis"},
)
def put_redis_data(context, process_data):
   context.resources.redis.put_data(process_data.date, process_data.high)
   

get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data],
    resource_defs={"redis": redis_resource, "s3": s3_resource},
    resource_config_by_key={
        "s3": {
            "config": {
                    "bucket": "dagster",
                    "access_key": "test",
                    "secret_key": "test",
                    "endpoint_url": "http://host.docker.internal:4566"
            }
        },
        "redis": {
            "config": {
                "host": 'redis',
                "port": 6379,
            }
        }
    }
)
