# Crude Oil Analysis with PySpark & Apache Iceberg

This project analyzes the U.S. Crude Oil Import dataset using PySpark and delivers results in csv or Apache Iceberg format (Bonus Question), which uses Docker as the container.

## Dataset
- Source: [Kaggle - U.S. Crude Oil Imports](https://www.kaggle.com/datasets/alistairking/u-s-crude-oil-imports/data)
- File used: `data.csv`

## Tasks & Solutions

### 1. Top 5 Destinations for Oil Produced in Albania, output in Apache iceberg format
- **Script:** `job1_top5_destinations.py`
- **Logic:**
  - Filter originName = "Albania" and originTypeName = "Country"
  - Group by destinationName, sum quantity
  - Order by total quantity descending, take top 5

### 2. For UK, Destinations with Total Quantity > 100,000
- **Script:** `job2_uk_gt_100k.py`
- **Logic:**
  - Filter originName = "UK" and originTypeName = "Country"
  - Group by destinationName, sum quantity > 100,000

### 3. Most Exported Grade per Year and Origin
- **Script:** `job3_most_exported_grade.py`
- **Logic:**
  - Group by year, originName, gradeName
  - Sum quantity
  - Use window function to get top grade per year and origin

### Bonus: Convert Result to Apache Iceberg Format
- **Script:** `job1_top5_destinations.py`
- **Output Table:** `local.db.top5_albania`
- **Iceberg Warehouse Path:** `./warehouse/db/top5_albania`

## Dockerized Setup

### 1. Build Docker Image
```bash
docker build -t crude-spark -f docker/Dockerfile .
```

### 2. Run with Volume Mounts & Iceberg Support
```bash
docker run --rm \
  -v $(pwd)/src:/app/src \
  -v $(pwd)/data:/data \
  -v $(pwd)/warehouse:/warehouse \
  crude-spark \
  spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.0 /app/src/job1_top5_destinations.py
```

## Output
- Console: Displays top 5 destinations with total quantity
- Files: Iceberg table written under `./warehouse/db/top5_albania`

## 📝 Notes
- Parquet output can be read locally via PySpark or Pandas. read_parquet.py is provided to read Parquet files.


