import logging
from json import JSONDecodeError
import os
from fastapi import FastAPI, status, Response, Request, Depends
from fastapi.security import HTTPBearer, HTTPBasicCredentials
from influxdb import InfluxDBClient
from geolib import geohash
from pythonjsonlogger import jsonlogger


logger = logging.getLogger(__name__)

db_host = os.getenv("HR_DB_HOST", "localhost")
db_port = os.getenv("HR_DB_PORT", "8086")
db_name = os.getenv("HR_DB_NAME", "health")
api_key = os.getenv("HR_API_KEY", "api_key_receiver")


def log_in_json() -> None:
    loggers = [
        logging.getLogger("uvicorn.access"),
        logging.getLogger("uvicorn.error"),
        logging.getLogger("uvicorn"),
        logging.getLogger(),
    ]
    for logger in loggers:
        for handler in logger.handlers:
            logger.removeHandler(handler)
        logger.level = logging.INFO
        log_handler = logging.StreamHandler()
        formatter = jsonlogger.JsonFormatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s"
        )
        log_handler.setFormatter(formatter)
        logger.addHandler(log_handler)


app = FastAPI()
log_in_json()

DATAPOINTS_CHUNK = 80000

client = InfluxDBClient(host=db_host, port=db_port)
client.create_database(db_name)
client.switch_database(db_name)

auth = HTTPBearer()


@app.post("/push")
async def push(
    request: Request,
    response: Response,
    authorization: HTTPBasicCredentials = Depends(auth),
):
    transformed_data = []

    if authorization.credentials != api_key:
        response.status_code = status.HTTP_403_FORBIDDEN
        return {"detail": "Auth Error"}

    try:
        healthkit_data = await request.json()
    except JSONDecodeError:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"message": "Invalid JSON Received", "error_code": 400}

    for metric in healthkit_data.get("data", {}).get("metrics", []):
        number_fields = []
        string_fields = []
        for datapoint in metric["data"]:
            metric_fields = set(datapoint.keys())
            metric_fields.remove("date")
            for mfield in metric_fields:
                if type(datapoint[mfield]) == int or type(datapoint[mfield]) == float:
                    number_fields.append(mfield)
                else:
                    string_fields.append(mfield)
            point = {
                "measurement": metric["name"],
                "time": datapoint["date"],
                "tags": {
                    str(nfield): str(datapoint[nfield]) for nfield in string_fields
                },
                "fields": {
                    str(nfield): float(datapoint[nfield]) for nfield in number_fields
                },
            }
            transformed_data.append(point)
            number_fields.clear()
            string_fields.clear()

    for i in range(0, len(transformed_data), DATAPOINTS_CHUNK):
        logger.info(
            msg={
                "op": "ingest_health_datapoint",
                "ingested_health_datapoints": len(
                    transformed_data[i : i + DATAPOINTS_CHUNK]
                ),
            }
        )
        client.write_points(transformed_data[i : i + DATAPOINTS_CHUNK])

    transformed_workout_data = []

    for workout in healthkit_data.get("data", {}).get("workouts", []):
        tags = {"id": workout["name"] + "-" + workout["start"] + "-" + workout["end"]}
        for gps_point in workout["route"]:
            point = {
                "measurement": "workouts",
                "time": gps_point["timestamp"],
                "tags": tags,
                "fields": {
                    "lat": gps_point["lat"],
                    "lng": gps_point["lon"],
                    "geohash": geohash.encode(gps_point["lat"], gps_point["lon"], 7),
                },
            }
            transformed_workout_data.append(point)

    for i in range(0, len(transformed_data), DATAPOINTS_CHUNK):
        logger.info(
            msg={
                "op": "ingest_workout_datapoints",
                "ingested_workout_datapoints": len(
                    transformed_data[i : i + DATAPOINTS_CHUNK]
                ),
            }
        )
        client.write_points(transformed_data[i : i + DATAPOINTS_CHUNK])

    return {
        "ingested_health_datapoints": len(transformed_data),
        "ingested_workout_datapoints": len(transformed_workout_data),
    }
