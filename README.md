# AppStore Analytics ETL Pipeline

## 📋 Описание проекта
Автоматизированный ETL-pipeline для анализа данных Apple Store. Данные из S3 загружаются через Airbyte в PostgreSQL, где преобразуются с помощью dbt в многослойную витрину данных. Весь процесс оркестрируется через Airflow.

## 🏗️ Архитектура системы

### Схема данных pipeline
```mermaid
graph TB
    S3[Amazon S3<br>Raw Apple Store Data]
    
    subgraph "Extract & Load (Airbyte)"
        A1[Airbyte Connection<br>S3 → PostgreSQL]
    end
    
    PG[PostgreSQL Database]
    
    subgraph "Transform (dbt)"
        STG[stg_layer<br>Raw data cleaning]
        INT[intermediate_layer<br>Business transformations]
        DM[data_mart_layer<br>Analytical models]
    end
    
    subgraph "Orchestration (Airflow)"
        AF[Airflow DAG<br>Coordinating pipeline]
    end
    

    S3 --> A1
    A1 --> PG
    PG --> STG
    STG --> INT
    INT --> DM

    
    AF -.-> A1
    AF -.-> STG
    AF -.-> INT
    AF -.-> DM
    
    style S3 fill:#ff9f43
    style A1 fill:#3498db
    style PG fill:#9b59b6
    style STG fill:#2ecc71
    style INT fill:#f1c40f
    style DM fill:#e74c3c
    style AF fill:#95a5a6
```

## 🧩 Компоненты системы

### 🔹 Источники данных
- **Amazon S3** - хранение сырых данных Apple Store

### 🔹 Загрузка данных (EL)
- **Airbyte** - синхронизация данных из S3 в PostgreSQL
- **Коннекторы**: S3 → PostgreSQL

### 🔹 Хранилище данных
- **PostgreSQL** - используется как DWH для хранения преобразованных данных

### 🔹 Трансформация данных (T)
- **dbt** - преобразование данных через три слоя:
  - **stg_layer** - очистка и стандартизация сырых данных
  - **intermediate_layer** - бизнес-трансформации и джойны
  - **data_mart_layer** - готовые аналитические модели

### 🔹 Оркестрация
- **Airflow** - управление всем ETL-пайплайном
- **DAGs** - автоматизация выполнения задач
## Предварительно создаем к БД apple_db

#### сначала создаем БД apple_dwh и открываем sql редактор в этой БД и выполняем дальнейший скрипт
```sql
select * from pg_catalog.pg_available_extensions;

create extension postgres_fdw;

drop server if exists data_db_pg cascade;
create server data_db_pg foreign data wrapper postgres_fdw options (
	host 'localhost',
	dbname 'data_db',
	port '5432'
);

create user mapping for postgres server data_db_pg options (
	user 'postgres',
	password 'postgres'
);

drop schema if exists data_db_src;
create schema data_db_src authorization postgres;
import foreign schema public from server data_db_pg into data_db_src;
```

## 🚀 Быстрый старт

1. **Клонирование репозитория**
```bash
git clone <repository-url>
cd appstore-analytics-pipeline
```