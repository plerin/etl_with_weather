<h1 align="center">etl_with_weather</h1>

<p align="center">
    <a href="#Goal">Goal</a> •
    <a href="#subject">Subject</a> •
    <a href="#Prerequisites">Prerequisites</a> •
    <a href="#Dag">Dag</a> •
    <a href="#Setting">Setting</a> •
    <a href="#Trouble_shooting">Trouble_shooting</a>
</p>
<br>

## About

대단한 걸 하려고 하지말자 결국 시작을 못하니까 이번 프로젝트에서 목표는 docker로 환경을 구성하고 airflow를 통해 DAG(ETL)을 구동할 것인데 그 과정에서 `1.` 2가지 방식(Full-refresh, incremental_update)으로 코드구성 `2.` PK\*제약사항 지켰는지 확인(중복, NULL, 최신 값) `3.` 데이터 수집 후 제대로 수집 됐는지 확인(CHECK) `4.` backfill 사용을 위한 설정(& 필요성) `5.` 에러 발생 시 slack으로 알람 `6.` cloud인 s3와 redshift를 사용한다 를 보여줄 예정

<br>

## Senario

### Architecture

<p align="center"><img src="https://raw.githubusercontent.com/plerin/plerin/main/project/etl_with_weather/architecture.png"></p>

Airflow으로 스케줄링된 DAG를 통해 매일 날씨 데이터를 수집 후 원본 데이터 그대로 S3에 적재, 그리고 S3 데이터에서 데이터를 추출하여 원하는 데이터만 파싱 후 redshift에 적재

1. Airflow scheduling을 통해 API(OpenWeather)에서 데이터 요청 및 S3 적재(raw_data)
2. S3 데이터에서 데이터 수집 후 원하는 데이터만 파싱하여 redshift에 적재

<br>

## Technical_Stack

- Docker
  - local에 개발 환경 구축
- Python
  - DAG 작성
- Airflow
  - Orchestration으로 DAG를 실행
- S3
  - Data_Lake 역할로 원본 데이터 적재
- Redshift
  - Datawarehouse 역할로 파싱된 데이터 적재

<br>

**Set-up**

- s3
- redshift
- slack_api(plugin)
- openweather_api

<br>

## Installation

docker compose up -d

in airflow WEB UI

input the connection, variable

run dag

result(view graph & result data)

## Process

Collect the data with API

1. Full_Refresh

1. Auto_Cremental

ETL from s3 to redshift
