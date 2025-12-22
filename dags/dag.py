from airflow.sdk import dag  # or from airflow.sdk.dag import dag
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from databricks import sql
from massive import RESTClient
import os

#get the environment variables
polygon_api_key = os.getenv("POLYGON_TOKEN")
server_hostname = os.getenv("DATABRICKS_HOST")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
access_token = os.getenv("DATABRICKS_TOKEN")

schema = 'jakebuto'


#helper function to execute queries in databricks
#prints the result of the query if fetch is set to True
def execute_databricks_query(query, fetch):
    #create the connector object using the databricks environment variables
    conn = sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token
    )
    #creates the cursor object, and uses it to run the query
    cursor = conn.cursor()
    cursor.execute(query)

    #if fetch is enabled, then print the results of the query
    if fetch:
        results = cursor.fetchall()
        for row in results:
            print(row)
        cursor.close()
        conn.close()
        return None

    #clean up the connection and cursor objects before returning
    cursor.close()
    conn.close()
    return None


@dag(
    description="Takes massive stock data in, stores it as a daily partition, and stores a 7-day cumulative partition",
    default_args={
        "owner": schema,
        "start_date": datetime(2025, 12, 17),
        "retries": 0,
        "execution_timeout": timedelta(hours=1),
    },
    start_date=datetime(2025, 12, 17),
    max_active_runs=1,
    schedule="@daily",
    catchup=False,
)
#
def stock_dag():
    #intiliaze variables used in the dag
    maang_stocks = ['AAPL', 'AMZN', 'NFLX', 'GOOGL', 'META']
    production_table = f'{schema}.daily_stock_prices'
    staging_table = production_table + '_stg_{{ ds_nodash }}'
    cumulative_table = f'{schema}.daily_stock_prices_cumulative'


    #gets stock related data from massive and inserts it into the staging table as a daily partition
    def load_data_from_polygon(table, **context):
        #intialize variables
        ds = context['ds']
        aggs = []
        client = RESTClient(api_key=polygon_api_key)

        print(f"Getting data for date: {ds}")

        #loops through the list of target stocks and gets available data from the massive api
        #inserts the data into the staging table
        for i in range(0, len(maang_stocks)):
            try:
                for a in client.list_aggs(
                    maang_stocks[i],
                    1,
                    "day",
                    ds,
                    ds,
                    limit=5,
                ):
                    query = f"""
                        INSERT INTO {staging_table}
                        (ticker, date, open, high, low, close, volume, vwap,
                        timestamp, transactions, insertion_timestamp)
                            VALUES (
                                '{maang_stocks[i]}',
                                DATE '{ds}',
                                {a.open},
                                {a.high},
                                {a.low},
                                {a.close},
                                {a.volume},
                                {a.vwap},
                                {a.timestamp},
                                {a.transactions},
                                CURRENT_TIMESTAMP
                            )
                        """
                    #calls the execute query function to run the insert into query
                    execute_databricks_query(query, False)

                    break
            #throws an exception if there is an issue fetching the data from the massive api
            except Exception as e:
                print(f"Error fetching {maang_stocks[i]}: {e}")

    #initial step to create the schema if it doesn't already exist
    create_schema_step = PythonOperator(
        task_id="create_schema_step",
        python_callable=execute_databricks_query,
        op_kwargs={
            'query': f"CREATE SCHEMA IF NOT EXISTS {schema}",
            'fetch': False
        }
    )


    #Creates the production table as a daily partition if it doesn't exist
    create_prod_table_step = PythonOperator(
        task_id="create_production_step",
        python_callable=execute_databricks_query,
        op_kwargs={
            'query': f"""
             -- Create production Iceberg table with date in name
             CREATE TABLE IF NOT EXISTS {production_table}
             (
                 ticker STRING,
                 date DATE,
                 open DECIMAL(10, 2),
                 high DECIMAL(10, 2),
                 low DECIMAL(10, 2),
                 close DECIMAL(10, 2),
                 volume BIGINT,
                 vwap DECIMAL(10, 2),
                 transactions INTEGER,
                 insertion_timestamp TIMESTAMP
             )
             USING ICEBERG
             PARTITIONED BY (date)
             COMMENT 'Production table for MAANG stock prices'
             """,
            'fetch': False
        }
    )

    #Creates the staging table using the same structure as the production table
    create_staging_table_step = PythonOperator(
        task_id="create_staging_step",
        python_callable=execute_databricks_query,  # Use YOUR function name
        op_kwargs={
            'query': f"""
                -- Create staging Iceberg table with date in name
                CREATE OR REPLACE TABLE {staging_table}
                (
                    ticker STRING,
                    date DATE,
                    open DECIMAL(10, 2),
                    high DECIMAL(10, 2),
                    low DECIMAL(10, 2),
                    close DECIMAL(10, 2),
                    volume BIGINT,
                    vwap DECIMAL(10, 2),
                    transactions INTEGER,
                    insertion_timestamp TIMESTAMP
                )
                USING ICEBERG
                COMMENT 'Staging table for {{ ds }} - will be dropped after load'
                """,
            'fetch': False

        }
    )

    #step to execute the load_data_from_polygon function
    #used to fill the staging table
    load_to_staging_step = PythonOperator(
        task_id="load_to_staging_step",
        python_callable=load_data_from_polygon,
        op_kwargs={
            'table': staging_table,
        },
    )

    #runs the following data quality checks:
    #1. checks that all MAANG stocks are present in the data
    #2. checks that there are no NULLS in the sotck price data
    #3. checks that there are no strange price ranges (i.e. low > high)
    #4. checks that there are no records with negative or zero volume
    #5. checks that the date value in the data matches the most recent complete day
    #6. checks that the stock tickers are valid
    run_dq_check = PythonOperator(
        task_id="run_dq_check",
        python_callable=execute_databricks_query,
        op_kwargs={
            'query': f"""
                -- ========== DATA QUALITY CHECKS for {{ ds }} ==========
                -- Check 1: All MAANG stocks present (5 total)
                SELECT 
                    'Missing stocks check' as check_name,
                    COUNT(DISTINCT ticker) as actual_count,
                    5 as expected_count,
                    CASE WHEN COUNT(DISTINCT ticker) = 5 THEN 'PASS' ELSE 'FAIL' END as status
                FROM {staging_table}
                WHERE date = DATE '{{{{ ds }}}}'

                UNION ALL
                
                -- Check 2: No NULL prices
                SELECT 
                    'NULL prices check' as check_name,
                    COUNT(*) as actual_count,
                    0 as expected_count,
                    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as status
                FROM {staging_table}
                WHERE date = DATE '{{{{ ds }}}}'
                AND (open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL)
            
                UNION ALL
                
                -- Check 3: Valid price ranges (high >= low, etc.)
                SELECT 
                    'Invalid price ranges' as check_name,
                    COUNT(*) as actual_count,
                    0 as expected_count,
                    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as status
                FROM {staging_table}
                WHERE date = DATE '{{{{ ds }}}}'
                AND (
                    high < low OR
                    open > high OR 
                    open < low OR
                    close > high OR
                    close < low
                )
                
                UNION ALL
            
                -- Check 4: Positive volume
                SELECT 
                    'Non-positive volume' as check_name,
                    COUNT(*) as actual_count,
                    0 as expected_count,
                    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as status
                FROM {staging_table}
                WHERE date = DATE '{{{{ ds }}}}'
                AND volume <= 0
            
                UNION ALL
                
                -- Check 5: Date matches execution date
                SELECT 
                    'Date mismatch' as check_name,
                    COUNT(*) as actual_count,
                    0 as expected_count,
                    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as status
                FROM {staging_table}
                WHERE date != DATE '{{{{ ds }}}}'
            
                UNION ALL
                
                -- Check 6: Stock tickers are valid (no weird symbols)
                SELECT 
                    'Invalid ticker format' as check_name,
                    COUNT(*) as actual_count,
                    0 as expected_count,
                    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as status
                FROM {staging_table}
                WHERE date = DATE '{{{{ ds }}}}'
                AND ticker NOT IN ('AAPL', 'AMZN', 'NFLX', 'GOOGL', 'META')
            """,

            'fetch': True

        }
    )


    #clears the production table for the given data
    #prevents duplicate data being written to production
    clear_step = PythonOperator(
        task_id="clear_step",
        depends_on_past=True,
        python_callable=execute_databricks_query,
        op_kwargs={
            'query': f"""
            -- Delete existing data for this date (idempotence)
            DELETE FROM {production_table}
            WHERE date = DATE '{{{{ ds }}}}'
            """,
            'fetch': False

        }
    )

    #takes the data from staging and moves it to production tables
    exchange_data_from_staging = PythonOperator(
        task_id="exchange_data_from_staging",
        python_callable=execute_databricks_query,
        op_kwargs={
            'query': f"""
                INSERT INTO {production_table}
                SELECT * FROM {staging_table}
                WHERE date = DATE('{{{{ ds }}}}')
            """,
            'fetch': False
        }
    )

    #drops the staging table after data is loaded to production
    #there is no use for the staging table in this dag after production data is successfully loaded
    drop_staging_table = PythonOperator(
        task_id="drop_staging_table",
        python_callable=execute_databricks_query,
        op_kwargs={
            'query': f"""
                -- Clean up staging table after successful load
                DROP TABLE IF EXISTS {staging_table}
                """,
            'fetch': False

        }
    )

    #Creates the cumulative table with a daily partition
    create_cumulative_step = PythonOperator(
        task_id="create_cumulative_step",
        python_callable=execute_databricks_query,
        op_kwargs={
            'query': f"""
                -- Create cumulative table for 7-day rolling metrics
                CREATE TABLE IF NOT EXISTS {cumulative_table}
                (
                    ticker STRING,
                    date DATE,
                    last_7_days_open ARRAY<DECIMAL(10, 2)>,
                    last_7_days_high ARRAY<DECIMAL(10, 2)>,
                    last_7_days_low ARRAY<DECIMAL(10, 2)>,
                    last_7_days_close ARRAY<DECIMAL(10, 2)>,
                    last_7_days_volume ARRAY<BIGINT>,
                    avg_7_day_volume DECIMAL(15, 2),
                    volatility_7_day DECIMAL(10, 4),
                    updated_at TIMESTAMP
                )
                USING ICEBERG
                PARTITIONED BY (date)
                COMMENT '7-day rolling window metrics for MAANG stocks'
                """,
            'fetch': False

        }
    )

    #clears the cumulative table for the given day
    #prevents dupicate data from being loaded
    clear_cumulative_step = PythonOperator(
        task_id="clear_cumulative_step",
        depends_on_past=True,
        python_callable=execute_databricks_query,
        op_kwargs={
            'query': f"""
             -- Delete existing data for this date (idempotence)
             DELETE FROM {cumulative_table}
             WHERE date = DATE '{{{{ ds }}}}'
             """,
            'fetch': False

        }
    )


    #takes the data from the production table, calculates cumulative metrics
    #and inserts them into the cumulative table
    cumulate_step = PythonOperator(
        task_id="cumulate_step",
        depends_on_past=True,
        python_callable=execute_databricks_query,
        op_kwargs={
            'query': f"""
                -- Calculate 7-day rolling arrays from production table
                INSERT INTO {cumulative_table}
                WITH daily_prices AS (
                    -- Get last 7 days of data (including today)
                    SELECT 
                        ticker,
                        date,
                        open,
                        high,
                        low,
                        close,
                        volume
                    FROM {production_table}
                    WHERE date >= DATE '{{{{ ds }}}}' - INTERVAL 7 DAYS
                    AND date <= DATE '{{{{ ds }}}}'
                    ORDER BY ticker, date
                ),
                rolling_windows AS (
                    SELECT
                        ticker,
                        date,
                        -- Create arrays of last 7 values (or fewer for first days)
                        ARRAY_AGG(open) OVER w as last_7_days_open,
                        ARRAY_AGG(high) OVER w as last_7_days_high,
                        ARRAY_AGG(low) OVER w as last_7_days_low,
                        ARRAY_AGG(close) OVER w as last_7_days_close,
                        ARRAY_AGG(volume) OVER w as last_7_days_volume,
                        -- Calculate 7-day average volume
                        AVG(volume) OVER w as avg_7_day_volume,
                        -- Calculate 7-day volatility (std deviation of closing prices)
                        STDDEV(close) OVER w as volatility_7_day
                    FROM daily_prices
                    WINDOW w AS (
                        PARTITION BY ticker
                        ORDER BY date
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    )
                )
                SELECT
                    ticker,
                    date,
                    last_7_days_open,
                    last_7_days_high,
                    last_7_days_low,
                    last_7_days_close,
                    last_7_days_volume,
                    avg_7_day_volume,
                    COALESCE(volatility_7_day, 0) as volatility_7_day,
                    CURRENT_TIMESTAMP
                FROM rolling_windows
                WHERE date = DATE '{{{{ ds }}}}'  -- Only insert today's calculated metrics
                """,
            'fetch': False

        }
    )

    #Dependency chain
    (
        create_schema_step
        >> [create_staging_table_step, create_prod_table_step, create_cumulative_step]
        >> load_to_staging_step
        >> run_dq_check
        >> clear_step
        >> exchange_data_from_staging
        >> [drop_staging_table, clear_cumulative_step >> cumulate_step]
    )

stock_dag()
