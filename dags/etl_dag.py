from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd
import os
import json
import mysql.connector
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    "owner": "Alex",
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

@dag(
    dag_id="daily_etl_pipeline_airflow3",
    description="ETL workflow demonstrating dynamic task mapping and assets",
    schedule="@daily",
    start_date=datetime(2026, 2, 21),
    catchup=False,
    default_args=default_args,
    tags=["airflow3", "etl"]
)

def daily_etl_pipeline():

  @task
  def extract_datasets(ti = None) -> dict:
    """Extracting datasets from a github repository and return a dictionary of DataFrames."""
    base_url = "https://raw.githubusercontent.com/al3x-id/datasets/refs/heads/main/datasets/"
    files = [
        "cust_info.csv", 
        "prd_info.csv",
        "sales_details.csv",
        "CUST_AZ12.csv",
        "LOC_A101.csv",
        "PX_CAT_G1V2.csv"
    ]

    os.makedirs("/opt/airflow/tmp", exist_ok=True)

    dataframes = {}
    for file in files:
        url = base_url + file
        df = pd.read_csv(url)
        raw_path = f"/opt/airflow/tmp/{file}"
        df.to_csv(raw_path, index=False)
        dataframes[file] = raw_path
        print(f"[EXTRACT] Dataset {file} saved at {raw_path}")

        summary = {
            "num_rows": len(df),
            "num_columns": len(df.columns),
            "columns": df.columns.tolist(),
            "sample_data": df.head(3).to_dict(orient="records")
        }
        formatted_summary = json.dumps(summary, indent=4, ensure_ascii=False).replace("\xa0", " ")

        if ti:
            ti.xcom_push(key=f"{file}_summary", value=formatted_summary)
            print("[XCOM] Pushed JSON summary to XCom")
    return dataframes
  

  @task
  def transform_datasets(raw_datasets: dict) -> dict:
      """
      Clean all raw datasets
      merge cleaned datasets into dimension and fact tables,
      save merge datasets as CSV files.
      """

      cust_info = pd.read_csv(raw_datasets["cust_info.csv"])
      prd_info = pd.read_csv(raw_datasets["prd_info.csv"])
      sales_details = pd.read_csv(raw_datasets["sales_details.csv"])
      cust_az12 = pd.read_csv(raw_datasets["CUST_AZ12.csv"])
      loc_a101 = pd.read_csv(raw_datasets["LOC_A101.csv"])
      px_cat_g1v2 = pd.read_csv(raw_datasets["PX_CAT_G1V2.csv"])
     
      def clean_cust_info(df):
        """Cleaning the cust_info dataset."""
        df["cst_create_date"] = pd.to_datetime(df["cst_create_date"], errors="coerce")
        df = df.sort_values(by="cst_create_date", ascending=False)
        df = df.drop_duplicates(subset=["cst_id"], keep="first", inplace=False)
        df = df.sort_values(by="cst_id", ascending=True)
        df = df[df["cst_id"].notna()]
        df["cst_id"] = df["cst_id"].astype(int)
        df["cst_firstname"] = df["cst_firstname"].str.strip().fillna("n/a")
        df["cst_lastname"] = df["cst_lastname"].str.strip().fillna("n/a")
        df["cst_marital_status"]= (
            df["cst_marital_status"]
            .map({"S": "Single", "M": "Married", "D": "Divorced"})
            .fillna("n/a")
        )
        df["cst_gndr"]= (
            df["cst_gndr"]
            .map({"M": "Male", "F": "Female"})
            .fillna("n/a")
        )
        return df
      def clean_prd_info(df):
          """Cleaning the prd_info dataset."""
          df["cat_id"] = df["prd_key"].str[:5].str.replace("-", "_")
          df["sls_prd_key"] = df["prd_key"].str[6:]
          df["prd_cost"] = df["prd_cost"].fillna(0).astype(int)
          df["prd_line"] = df["prd_line"].str.strip()
          df["prd_line"] = (
              df["prd_line"]
              .map({"R": "Road", "S": "Others", "M": "Mountain", "T": "Touring"})
              .fillna("n/a")
          )
          df = (
              df
              .assign(
                  prd_start_dt=lambda df: pd.to_datetime(df["prd_start_dt"], errors="coerce")
              )
              .drop(columns="prd_end_dt")
          )
          df["prd_end_dt"] = (
              df.groupby("prd_key")["prd_start_dt"]
                  .shift(-1) - pd.Timedelta(days=1)
          )
          df =df.iloc[:, [0, 1, 6, 7, 2, 3, 4, 5, 8]]
          return df
      
      def clean_sales_details(df):
          """Cleaning the sales_details dataset."""
          date_cols = ["sls_order_dt", "sls_ship_dt", "sls_due_dt"]
          for col in date_cols:
              df[col] = pd.to_datetime(
                  df[col].replace(0, pd.NA).astype(str),
                  format="%Y%m%d",
                  errors="coerce"
              )
          df["sls_sales"] = df["sls_sales"].replace (0, pd.NA).fillna(
              df["sls_quantity"] * df["sls_price"]
          ).astype(int)
          df["sls_price"] = df["sls_price"].fillna(
              df["sls_sales"] /
              df["sls_quantity"]
          ).astype(int)
          return df

      def clean_cust_az12(df):
          """Cleaning the cust_az12 dataset."""
          df["CID"] = df["CID"].str.removeprefix("NAS")
          df["BDATE"] = pd.to_datetime(df["BDATE"], errors="coerce")
          df["GEN"] = (
              df["GEN"]
              .str.strip()
              .replace({"M": "Male", "F": "Female"})
              .fillna("n/a")
          )
          return df

      def clean_loc_a101(df):
          """Cleaning the loc_a101 dataset."""
          df["CID"] = df["CID"].str.replace("-", "")
          return df

      def clean_px_cat_g1v2(df):
          """Cleaning the px_cat_g1v2 dataset."""
          str_cols = ["CAT", "SUBCAT", "MAINTENANCE"]
          for col in str_cols:
              df[col] = df[col].str.strip().fillna("n/a")
          return df

      cust_info = clean_cust_info(cust_info)
      prd_info = clean_prd_info(prd_info)
      sales_details = clean_sales_details(sales_details)
      cust_az12 = clean_cust_az12(cust_az12)
      loc_a101 = clean_loc_a101(loc_a101)
      px_cat_g1v2 = clean_px_cat_g1v2(px_cat_g1v2)

      print("[TRANSFORM] Datasets cleaned and transformed successfully.")

      customers = pd.merge(
        pd.merge(cust_info, cust_az12, left_on="cst_key", right_on="CID", how="left"),
        loc_a101,
        on="CID",
        how="left")    
      customers["gender"] = (
          customers["cst_gndr"]
          .replace({"n/a", pd.NA})
          .combine_first(customers["GEN"])
          .fillna("n/a")
      )
      customers = customers.drop(columns=["cst_gndr", "CID", "GEN"])
      customers = customers.sort_values(by="cst_id").reset_index(drop=True)
      customers["customer_key"] = customers.index + 1
      customers = customers.rename(columns={
          "cst_id": "customer_id",
          "cst_key": "customer_number", 
          "cst_firstname": "first_name", 
          "cst_lastname": "last_name", 
          "cst_marital_status": "marital_status", 
          "cst_create_date": "create_date", 
          "BDATE": "birthdate", 
          "CNTRY": "country"}
            ) 
      dim_customers = customers.iloc[:, [9, 0, 1, 2, 3, 8, 4, 7, 6, 5]]
      

      products = prd_info[prd_info["prd_end_dt"].isna()].drop(columns=["prd_key","prd_end_dt"])
      products = pd.merge(products, px_cat_g1v2, left_on="cat_id", right_on="ID", how="left").drop(columns=["ID"])  
      products = products.sort_values(by="prd_id").reset_index(drop=True)
      products["product_key"] = products.index + 1
      products = products.rename(columns={
      "prd_id": "product_id",
      "sls_prd_key": "product_number",
      "prd_nm": "product_name",
      "cat_id": "category_id",
      "MAINTENANCE": "maintenance",
      "CAT": "category",
      "SUBCAT": "subcategory",
      "prd_line": "product_line",
      "prd_start_dt": "start_date",
      "prd_cost": "cost"
  })
      dim_products = products.iloc[:, [10, 0, 2, 3, 1, 7, 8, 9, 5, 4, 6]]

      sales = pd.merge(
      pd.merge(sales_details, dim_customers, left_on="sls_cust_id", right_on="customer_id", how="left"),
      dim_products, left_on="sls_prd_key", right_on="product_number", how="left"
      )
      sales = sales.drop(columns=[
      "sls_prd_key", "sls_cust_id", 
      "customer_id", "customer_number","first_name", "last_name", "marital_status", 
      "gender", "country","birthdate", "create_date", "product_id", "product_name", "product_line",
      "cost", "start_date", "product_number", "category_id", "category","subcategory", "maintenance"])
      sales = sales.rename(columns={
      "sls_ord_num": "order_number",
      "sls_order_dt": "order_date",
      "sls_ship_dt": "ship_date",
      "sls_due_dt": "due_date",
      "sls_sales": "sales_amount",
      "sls_quantity": "quantity",
      "sls_price": "price",
      "sls_date": "date"
      })
      fact_sales = sales.iloc[:, [0, 7, 8, 1, 2, 3, 4, 5, 6]]

      merged_datasets = [
          "dim_customers",
          "dim_products",
          "fact_sales",
      ]

      merged_paths = {}
      for name in merged_datasets:
          df = locals()[name]
          path = f"/opt/airflow/tmp/{name}.csv"
          df.to_csv(path, index=False)
          merged_paths[name] = path
          print(f"[TRANSFORM] {name} saved at {path}")

      return merged_paths
  
  @task
  def load_to_mysql(transformed_datasets: dict):
      """Load the transformed datasets into MySQL tables."""
      db_config = {
          "host": "host.docker.internal",
          "user": "airflow",
          "password": "airflow",
          "database": "dim_fact_db",
          "port": 3306
      }
      merged_datasets = [
          "dim_customers",
          "dim_products",
          "fact_sales",
      ]

      schema = {
          "dim_customers": """
              customer_key INT PRIMARY KEY,
              customer_id INT,
              customer_number VARCHAR(255),
              first_name VARCHAR(255),
              last_name VARCHAR(255),
              gender VARCHAR(255),
              marital_status VARCHAR(255),
              country VARCHAR(255),
              birthdate DATE,
              create_date DATE
          """,
          "dim_products": """
              product_key INT PRIMARY KEY,
              product_id INT,
              product_number VARCHAR(255),
              product_name VARCHAR(255),
              category_id VARCHAR(255),
              category VARCHAR(255),
              subcategory VARCHAR(255),
              maintenance VARCHAR(255),
              product_line VARCHAR(255),
              cost DECIMAL(10, 2),
              start_date DATE
          """,
          "fact_sales": """
              order_number VARCHAR(255),
              customer_key INT,
              product_key INT,
              order_date DATE,
              ship_date DATE,
              due_date DATE,
              sales_amount DECIMAL(10, 2),
              quantity INT,
              price DECIMAL(10, 2)
          """
      }

      conn = mysql.connector.connect(**db_config)
      cursor = conn.cursor()
      

      for name in merged_datasets:
          df = pd.read_csv(transformed_datasets[name])
          df = df.replace({pd.NA: None, float("nan"): None})

          cursor.execute(f"DROP TABLE IF EXISTS {name}")
          cursor.execute(f"CREATE TABLE {name} ({schema[name]});")
          cursor.execute(f"TRUNCATE TABLE {name}")

          sql = f"""
              INSERT INTO {name} ({', '.join(df.columns)})
              VALUES ({', '.join(['%s'] * len(df.columns))})
            """
          data = [tuple(row) for row in df.to_numpy()]
          cursor.executemany(sql, data)
          print(f"[LOAD] Data successfully loaded into MySQL table: {name}")

      conn.commit()
      cursor.close()
      conn.close()
      
  raw_datasets = extract_datasets()
  transformed_datasets = transform_datasets(raw_datasets)
  load_to_mysql(transformed_datasets)
dag = daily_etl_pipeline()