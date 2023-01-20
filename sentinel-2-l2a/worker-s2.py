################
# ARD workflow #
################

import json
import redis
from workflows.utils.prepS2 import prepareS2

def process_scene(json_data):
    loaded_json = json.loads(json_data)
    prepareS2(**loaded_json)

##################
# Job processing #
##################

import os
import logging
import datetime

if __name__ == "__main__":
    try:
        level = os.getenv("LOGLEVEL", "INFO").upper()
        logging.basicConfig(format="%(asctime)s %(levelname)-8s %(name)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S", level=level)
        host = os.getenv("REDIS_HOST", "localhost")
        port = int(os.getenv("REDIS_PORT", "6379"))
        host = os.getenv("REDIS_HOST", "redis-master")
        redis_queue = os.getenv("REDIS_S2_PROCESSED_CHANNEL", "jobS2")
        redis = redis.Redis(host=host, port=port)
        log_file_name = f"landsat_ard_{datetime.datetime.now()}.log"
        log_file_path = f"/tmp/{log_file_name}"
        logging_file_handler = logging.FileHandler(log_file_path)
        logging.getLogger().addHandler(logging_file_handler)
        logger = logging.getLogger("worker")

        while True:
            item = redis.blpop(redis_queue,timeout=1)
            if item is not None:
                itemstr = item[1].decode("utf=8")
                logger.info(f"Working on {itemstr}")
                start = datetime.datetime.now().replace(microsecond=0)
                loaded_json = json.loads(itemstr)
                prepareS2(**loaded_json)
                end = datetime.datetime.now().replace(microsecond=0)
                logger.info(f"Total processing time {end - start}")
            else:
                logger.info("No work found in queue")
                break
            logger.info("Queue empty, exiting")
            exit(0)
    except Exception as e:
        logger.exception(e)
        logging.getLogger().removeHandler(logging_file_handler)
        logging_file_handler.close()

