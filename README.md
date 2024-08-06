# Data Engineering Camp Capstone Project

## **Earthquakes ELT**
_Joshua Botticher_

## Objective:
The objective of this project is to create a real-time monitoring system that visualizes earthquake data, and dashboards using historical data from the USGS Earthquake API for public use.

## Consumers:
- Researchers: They use the data for analyzing seismic activity patterns and predicting future events.
- Students: They need access to earthquake data for educational projects and learning purposes.
- Educators: They require the data to teach seismic activity and its impacts.
- General Public: They seek to understand earthquake risks and safety measures.

## Questions We Want To Answer:
1) What are the most recent earthquakes and their magnitudes?
2) Which regions are experiencing the highest frequency of earthquakes?
3) What are the patterns and trends in seismic activity over the past year?
4) Which areas are most at risk based on historical data?


| `Source Name`  | `Source Type` | `Source Docs`                               | `Endpoint` |
| -------------  | ------------- | ------------                                | -----------|
|  USGS Earthquake Catalog    | rest api      | https://earthquake.usgs.gov/fdsnws/event/1/ | https://earthquake.usgs.gov/fdsnws/event/1/query|


## Architecture:
<img width="668" alt="Architecture" src="https://github.com/user-attachments/assets/1055687a-fff1-47a5-b144-bdfdd57ea103">



## Modeling
![dim_model_earthquakes drawio](https://github.com/user-attachments/assets/773c7714-f0e6-408d-b522-f668ba466f00)

