# [Data Engineer Camp](https://dataengineercamp.com): Capstone Project

## **Earthquakes ELT: Production Version**
Author: _Joshua Botticher_

** See Local_Earthquake_ELT repo for Local code version **

## Objective:
The objective of this project is to create a real-time monitoring system that visualizes earthquake data, and dashboards using historical data from the USGS Earthquake API for public use.

## Consumers:
- Researchers: They use the data for analyzing seismic activity patterns and predicting future events.
- Students: They need access to earthquake data for educational projects and learning purposes.
- Educators: They require the data to teach seismic activity and its impacts.
- General Public: They seek to understand earthquake risks and safety measures.

## Questions I Want To Answer:
1) What are the most recent earthquakes and their magnitudes?
2) Which regions are experiencing the highest frequency of earthquakes?
3) What are the patterns and trends in seismic activity over the past year?
4) Which areas are most at risk based on historical data?


| `Source Name`  | `Source Type` | `Source Docs`                               | `Endpoint` |
| -------------  | ------------- | ------------                                | -----------|
|  USGS Earthquake Catalog    | rest api      | https://earthquake.usgs.gov/fdsnws/event/1/ | https://earthquake.usgs.gov/fdsnws/event/1/query|


## Architecture:
![architecture_earthquakes drawio-3](https://github.com/user-attachments/assets/ee4deb94-5733-4279-b6d0-1a2c31fd109c)

## Instructions:
1) Clone the repo.
2) Deploy Airbyte on your local. See Airbyte docs: https://docs.airbyte.com/deploying-airbyte/docker-compose
3) Deploy Dagster on your local. See dagster docs: https://docs.dagster.io/guides/running-dagster-locally
4) Create an AWS account and create an S3 bucket. See AWS docs: https://docs.aws.amazon.com/AmazonS3/latest/userguide/GetStartedWithS3.html#creating-bucket
5) Update the .env variables as needed.
