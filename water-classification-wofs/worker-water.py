#!/usr/bin/env python

################
# ARD workflow #
################

import json
from workflows.utils.genprepWater import per_scene_wofs

def process_scene(json_data):
    loaded_json = json.loads(json_data)
    per_scene_wofs(**loaded_json)

##################
# Job processing #
##################

import os
import logging
import datetime
import redis

if __name__ == "__main__":
    log_file_name = f"landsat_ard_{datetime.datetime.now()}.log"
    log_file_path = f"/tmp/{log_file_name}"
    level = os.getenv("LOGLEVEL", "INFO").upper()
    logging.basicConfig(format="%(asctime)s %(levelname)-8s %(name)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
                        level=level)
    logging.getLogger().addHandler(logging.StreamHandler())
    logging_file_handler = logging.FileHandler(log_file_path)
    logging.getLogger().addHandler(logging_file_handler)
    logger = logging.getLogger("worker")

    try:
        level = os.getenv("LOGLEVEL", "INFO").upper()
        logging.basicConfig(format="%(asctime)s %(levelname)-8s %(name)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
                            level=level)
        host = os.getenv("REDIS_HOST", "localhost")
        port = int(os.getenv("REDIS_PORT", "30000"))
        redis_queue = "jobWater"
        redis = redis.Redis(host=host, port=port)

        while True:
            item = redis.blpop(redis_queue, timeout=10)
            if item is not None:
                itemstr = item[1].decode("utf=8")
                logger.info(f"Working on {itemstr}")
                start = datetime.datetime.now().replace(microsecond=0)
                loaded_json = json.loads(itemstr)
                per_scene_wofs(**loaded_json)
                end = datetime.datetime.now().replace(microsecond=0)
                logger.info(f"Total processing time {end - start}")
            else:
                logger.info("No work found in queue")
                #break
                
        logger.info("Queue empty, exiting")
        exit(0)

    except Exception as e:
        logger.exception(e)
        logging.getLogger().removeHandler(logging_file_handler)
        logging_file_handler.close()
