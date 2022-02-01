<h1 align="center">etl_with_weather</h1>

<p align="center">
    <a href="#Goal">Goal</a> •
    <a href="#Senario">Senario</a> •
    <a href="#Technical_Stack">Technical_Stack</a> •
    <a href="#Set-up">Set-up</a> •
    <a href="#Process">Process</a>
</p>
<br>

# Goal

### 날씨를 주제로 수집부터 가공에 이르는 데이터 파이프라인 구성.

<br>

`1.` Docker 기반 개발환경 구축 <br>
`2.` Cloud Service 활용 <br>
`3.` 데이터 유형에 따른 2가지 방식 etl 코드 작성 <br>
`4.` Data-Warehouse 활용하기 위한 PK 제약사항 추가 <br>
`5.` 데이터 수집 후 QUALITY 확인 <br>
`6.` DAG 구동 중 에러 발생 확인 <br>

<br>

# Senario

<br>

### **Architecture**

<p align="center"><img src="https://raw.githubusercontent.com/plerin/plerin/main/project/etl_with_weather/architecture.png"></p>

<br>

### **Data_info**

- **`provider`** : open_weather

- **`subject`** : weather data

- **`cycle`** : daily

<br>

Airflow으로 스케줄링된 DAG를 통해 매일 날씨 데이터를 수집 후 원본 데이터 그대로 S3에 적재, 그리고 그와 별개로 원본 데이터를 변환(원하는 필드만 파싱)하여 데이터 웨어하우인 redshift에 적재

1. Airflow scheduling을 통해 API(OpenWeather)에서 데이터 요청 및 S3 적재(raw_data)
2. 수집한 데이터를 변환하여 redshift에 적재

<br>

# Technical_Stack

- Docker
  - env, 개발 환경 구축(for Airflow)
- Python
  - Language, DAG 작성
- Airflow
  - Orchestration, DAG를 실행
- S3
  - Data-Lake, 원본 데이터 적재
- Redshift
  - Data-warehouse, 수집 후 변환된 데이터 적재

<br>

# Set-up

### AWS

- path : airflow-data/creds/s3

      [aws-info]
      aws_access_key_id = {aws_access_key}
      aws_secret_access_key = {aws_sercet_access_key}

### s3 / open_weather_api_key / slack_api_key

using airflow's `Variable`

- **`s3`** : bucket's name
- **`slack`** : api_key & token_key
- **`open_weather`** : api_key

  <br>

  <p align="center"><img src="https://raw.githubusercontent.com/plerin/plerin/main/project/etl_with_weather/variable_info.png"></p>

### redshift

using airflow's `Connection`

  <br>

  <p align="center"><img src="https://raw.githubusercontent.com/plerin/plerin/main/project/etl_with_weather/connection_info.png"></p>

<br>

# Process

  <p align="center"><img src="https://raw.githubusercontent.com/plerin/plerin/main/project/etl_with_weather/dag.png"></p>

<image>

**Task `get_data_by_api`**
<br>

Collecting data using api request

```python
API_URL = "https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&exclude={exclude}&appid={api_key}&units=metric"

 response = requests.get(
        API_URL.format(
            lat=COORD['lat'],
            lon=COORD['lon'],
            exclude=EXCLUDE,
            api_key=API_KEY
        )
    )
```

**Task `check_data`**

Check requesting data

- if data is none -> call `endRun`
- else -> `load_into_s3`

```python
    fetchedDate = ti.xcom_pull(key='return_value', task_ids=[
                               'get_data_by_api'])

    if fetchedDate[0] is not None:
        return 'load_into_s3'
    return 'endRun'
```

**Task `load_into`**

file name format is `%Y%m%d.json ex) 20220101.json`

if already exist file in s3 then delete file(key) before executing insert query

```python
    key = execution_date.strftime('%Y%m%d') + '.json'

    hook = S3Hook()
    bucket = s3_config['bucket']

    obj = hook.get_key(key, bucket_name=bucket)

    if obj:
        hook.delete_objects(bucket, key)
        logging.info('Delete key name ' + key)

    hook.load_string(data, key, bucket_name=bucket)

```

**Task `transform`**

pull data using xcom(task `get_data_by_api`)

loop data with daily_data(d['daily'])

**Field_desc**

- d['dt'] = 일자
- d['temp']['day'] = 평균기온
- d['temp']['min'] = 최저온도
- d['temp']['max'] = 최고온도

```python
    data = context["task_instance"].xcom_pull(
        key="return_value", task_ids="get_data_by_api")

    j_data = json.loads(data)

    ret = []
    for d in j_data["daily"]:
        day = datetime.fromtimestamp(d["dt"]).strftime('%Y-%m-%d')
        ret.append("('{}', {}, {}, {})".format(
            day, d["temp"]["day"], d["temp"]["min"], d["temp"]["max"]))

```

**Task `load_into`**

Upsert data

1. creat temp table and insert data in origin_table
2. insert into temp_table from collecting data
3. delete origin_table and insert recent data in temp_table

```python

create_sql = f"""DROP TABLE IF EXISTS {schema}.temp_{table};
    CREATE TABLE {schema}.temp_{table} (LIKE {schema}.{table} INCLUDING DEFAULTS); INSERT INTO {schema}.temp_{table} SELECT * FROM {schema}.{table}"""


insert_sql = f"""INSERT INTO {schema}.temp_{table} VALUES """ + \
        ",".join(weather_data)


alter_sql = f"""DELETE FROM {schema}.{table};
    INSERT INTO {schema}.{table}
    SELECT date, temp, min_temp, max_temp FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY date ORDER BY updated_date DESC) seq
        FROM {schema}.temp_{table}
    )
    WHERE seq = 1;"""

```

## License

You can check out the full license [here](https://github.com/plerin/etl_with_weather/blob/main/LICENSE)

This project is licensed under the terms of the **MIT** license.
