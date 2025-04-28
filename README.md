# pr_08
# Практическая работа 8. Анализ метода загрузки данных
# Цель
Определить наиболее эффективный метод загрузки данных (малых и больших объемов) из CSV-файлов в СУБД PostgreSQL, сравнивая время выполнения для методов: pandas.to_sql(), psycopg2.copy_expert() (с файлом и с io.StringIO), и пакетная вставка (psycopg2.extras.execute_values).
# Задачи
1. Подключиться к предоставленной базе данных PostgreSQL.
2. Проанализировать структуру исходных CSV-файлов (upload_test_data.csv , upload_test_data_big.csv).
3. Создать эскизы ER-диаграмм для таблиц, соответствующих структуре CSV-файлов.
4. Реализовать три различных метода загрузки данных в PostgreSQL(pandas.to_sql(), copy_expert(), io.StringIO).
5. Измерить время, затраченное каждым методом на загрузку данных из малого файла (upload_test_data.csv).
6. Измерить время, затраченное каждым методом на загрузку данных из большого файла (upload_test_data_big.csv).
7. Визуализировать результаты сравнения времени загрузки с помощью гистограммы (matplotlib).
8. Сделать выводы об эффективности каждого метода для разных объемов данных.
# Выполнение работы
Выполним подключение к обновленным библиотекам
````
%pip install psycopg2-binary pandas sqlalchemy matplotlib numpy
````
Получаем результат
![image](https://github.com/user-attachments/assets/6bcb2147-8cda-47cf-aef5-65cdf0b0ba73)
Подключаем библиотеки для работы с базами данных, анализа данных и визуализации
````
small_csv_path = r'C:\Users\valer\Downloads\upload_test_data.csv'
````
````
big_csv_path = r'C:\Users\valer\Downloads\upload_test_data_big.csv'
````
````
!ls
````
````
# @markdown Установка и импорт необходимых библиотек.


print("Libraries installed and imported successfully.")

# Database connection details (replace with your actual credentials if different)
DB_USER = "postgres"
DB_PASSWORD = "1"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "lect_08_bda_big_data"

# CSV File Paths (Ensure these files are uploaded to your Colab environment)
small_csv_path = 'upload_test_data.csv'
big_csv_path = 'upload_test_data_big.csv' # Corrected filename

# Table name in PostgreSQL
table_name = 'sales_data'
````
Проведем сравнение скорости загрузки данных в PostgreSQL разными методами (pandas.to_sql, copy_expert из файла и StringIO) для маленького и большого CSV-файлов, с замером времени выполнения.
````
# @title # 6. Data Loading Methods Implementation and Timing
# @markdown Реализация и измерение времени для каждого метода загрузки.

# Dictionary to store timing results
timing_results = {
    'small_file': {},
    'big_file': {}
}

# Check if files exist before proceeding
if not os.path.exists(small_csv_path):
    print(f"ERROR: Small CSV file not found: {small_csv_path}. Upload it and restart.")
elif not os.path.exists(big_csv_path):
    print(f"ERROR: Big CSV file not found: {big_csv_path}. Upload it and restart.")
elif not connection or not cursor or not engine:
    print("ERROR: Database connection not ready. Cannot proceed.")
else:
    # --- Method 1: pandas.to_sql() ---
    def load_with_pandas_to_sql(eng, df, tbl_name, chunk_size=1000):
        """Loads data using pandas.to_sql() and returns time taken."""
        start_time = time.perf_counter()
        try:
            # Using method='multi' might be faster for some DBs/data
            # Chunksize helps manage memory for large files
            df.to_sql(tbl_name, eng, if_exists='append', index=False, method='multi', chunksize=chunk_size)
        except Exception as e:
             print(f"Error in pandas.to_sql: {e}")
             # Note: No explicit transaction management here, relies on SQLAlchemy/DBAPI defaults or engine settings.
             # For critical data, wrap in a try/except with explicit rollback if needed.
             raise # Re-raise the exception to signal failure
        end_time = time.perf_counter()
        return end_time - start_time

    # --- Method 2: psycopg2.copy_expert() with CSV file ---
    def load_with_copy_expert_file(conn, cur, tbl_name, file_path):
        """Loads data using psycopg2.copy_expert() directly from file and returns time taken."""
        start_time = time.perf_counter()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # Skip header row using COPY options
                sql_copy = f"""
                COPY {tbl_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')
                """
                cur.copy_expert(sql=sql_copy, file=f)
            conn.commit() # Commit transaction after successful COPY
        except (Exception, Error) as error:
            print(f"Error in copy_expert (file): {error}")
            conn.rollback() # Rollback on error
            raise
        end_time = time.perf_counter()
        return end_time - start_time

    # --- Method 3: psycopg2.copy_expert() with io.StringIO ---
    def load_with_copy_expert_stringio(conn, cur, df, tbl_name):
        """Loads data using psycopg2.copy_expert() from an in-memory StringIO buffer and returns time taken."""
        start_time = time.perf_counter()
        buffer = io.StringIO()
        # Write dataframe to buffer as CSV, including header
        df.to_csv(buffer, index=False, header=True, sep=',')
        buffer.seek(0) # Rewind buffer to the beginning
        try:
            sql_copy = f"""
            COPY {tbl_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')
            """
            cur.copy_expert(sql=sql_copy, file=buffer)
            conn.commit() # Commit transaction after successful COPY
        except (Exception, Error) as error:
            print(f"Error in copy_expert (StringIO): {error}")
            conn.rollback() # Rollback on error
            raise
        finally:
            buffer.close() # Ensure buffer is closed
        end_time = time.perf_counter()
        return end_time - start_time


    # --- Timing Execution ---
    print("\n--- Starting Data Loading Tests ---")

    # Load DataFrames (only once)
    print("Loading CSV files into Pandas DataFrames...")
    try:
        df_small = pd.read_csv(small_csv_path)
        # The big file might be too large to load fully into Colab memory.
        # If memory errors occur, consider processing it in chunks for methods
        # that support it (like pandas.to_sql with chunksize, or modify batch insert).
        # For COPY methods, memory isn't usually an issue as they stream.
        df_big = pd.read_csv(big_csv_path)
        print(f"Loaded {len(df_small)} rows from {small_csv_path}")
        print(f"Loaded {len(df_big)} rows from {big_csv_path}")
    except MemoryError:
        print("\nERROR: Not enough RAM to load the large CSV file into a Pandas DataFrame.")
        print("Some methods (pandas.to_sql, StringIO, Batch Insert) might fail or be inaccurate.")
        print("The copy_expert (file) method should still work.")
        # We can try to proceed, but note the limitation
        df_big = None # Indicate that the big dataframe couldn't be loaded
    except Exception as e:
        print(f"Error loading CSVs into DataFrames: {e}")
        df_small, df_big = None, None # Stop execution if loading fails


    if df_small is not None: # Proceed only if small DF loaded
        # --- Small File Tests ---
        print(f"\n--- Testing with Small File ({small_csv_path}) ---")

        # Test pandas.to_sql
        try:
            reset_table(connection, cursor, table_name)
            print("Running pandas.to_sql...")
            t = load_with_pandas_to_sql(engine, df_small, table_name)
            timing_results['small_file']['pandas.to_sql'] = t
            print(f"Finished in {t:.4f} seconds.")
        except Exception as e: print(f"pandas.to_sql failed for small file.")

        # Test copy_expert (file)
        try:
            reset_table(connection, cursor, table_name)
            print("Running copy_expert (file)...")
            t = load_with_copy_expert_file(connection, cursor, table_name, small_csv_path)
            timing_results['small_file']['copy_expert (file)'] = t
            print(f"Finished in {t:.4f} seconds.")
        except Exception as e: print(f"copy_expert (file) failed for small file.")

        # Test copy_expert (StringIO)
        try:
            reset_table(connection, cursor, table_name)
            print("Running copy_expert (StringIO)...")
            t = load_with_copy_expert_stringio(connection, cursor, df_small, table_name)
            timing_results['small_file']['copy_expert (StringIO)'] = t
            print(f"Finished in {t:.4f} seconds.")
        except Exception as e: print(f"copy_expert (StringIO) failed for small file.")



    # --- Big File Tests ---
    print(f"\n--- Testing with Big File ({big_csv_path}) ---")

    # Test pandas.to_sql (if df_big loaded)
    if df_big is not None:
        try:
            reset_table(connection, cursor, table_name)
            print("Running pandas.to_sql...")
            t = load_with_pandas_to_sql(engine, df_big, table_name, chunk_size=10000) # Larger chunksize for big file
            timing_results['big_file']['pandas.to_sql'] = t
            print(f"Finished in {t:.4f} seconds.")
        except Exception as e: print(f"pandas.to_sql failed for big file.")
    else:
        print("Skipping pandas.to_sql for big file (DataFrame not loaded).")


    # Test copy_expert (file) - This should work even if df_big didn't load
    try:
        reset_table(connection, cursor, table_name)
        print("Running copy_expert (file)...")
        t = load_with_copy_expert_file(connection, cursor, table_name, big_csv_path)
        timing_results['big_file']['copy_expert (file)'] = t
        print(f"Finished in {t:.4f} seconds.")
    except Exception as e: print(f"copy_expert (file) failed for big file.")


    # Test copy_expert (StringIO) (if df_big loaded)
    if df_big is not None:
        try:
            reset_table(connection, cursor, table_name)
            print("Running copy_expert (StringIO)...")
            t = load_with_copy_expert_stringio(connection, cursor, df_big, table_name)
            timing_results['big_file']['copy_expert (StringIO)'] = t
            print(f"Finished in {t:.4f} seconds.")
        except Exception as e: print(f"copy_expert (StringIO) failed for big file.")
    else:
        print("Skipping copy_expert (StringIO) for big file (DataFrame not loaded).")



    print("\n--- Data Loading Tests Finished ---")

# Final check of results dictionary
print("\nTiming Results Summary:")
import json
print(json.dumps(timing_results, indent=2))
````
Получаем следующий результат и визуализацию результатов сравнения времени загрузок
![image](https://github.com/user-attachments/assets/bc7d4d62-984b-4839-a759-8c59f3abc399)
![image](https://github.com/user-attachments/assets/da61c4f1-85fb-46d2-9550-a2b4d3878f43)
# Индивидуальные задания. Вариант 9
1. Настройка таблиц. Создать таблицы sales_small, sales_big.
2. Загрузка малых данных. Метод: copy_expert (StringIO)
3. Загрузка больших данных. Метод: copy_expert (file)
4. SQL Анализ	SQL: Вычислить средний cost в sales_big, где quantity = 50.
5. Python/Colab Анализ и Визуализация.  Python: Загрузить big_csv_path (если возможно), рассчитать медианный cost. Сравнить со средним из SQL.

Для начала проверяем выполнение всех вспомогательных функций из шаблона

Получаем результат
![image](https://github.com/user-attachments/assets/4bd549b7-659c-4ae6-a333-8dcefe412a43)
# Задание 1. Настройка таблиц. Создать таблицы sales_small, sales_big.
````
print("\n--- Задача 1: Создание таблиц ---")
create_table(small_table_name)
create_table(big_table_name)
````
Получаем результат
![image](https://github.com/user-attachments/assets/e1635159-1214-4af6-9da1-d2fc7438ac35)

# Задание 2. Загрузка малых данных. Метод: copy_expert (StringIO)
````
print(f"\n--- Задача 2: Загрузка данных из '{small_csv_path}' в '{small_table_name}' (метод StringIO) ---")
    if os.path.exists(small_csv_path):
        try:
            # Шаг 1: Читаем CSV в DataFrame (необходимо для StringIO)
            print(f"Чтение {small_csv_path} в DataFrame...")
            df_small_for_load = pd.read_csv(small_csv_path)
            print(f"Прочитано {len(df_small_for_load)} строк.")

            # Шаг 2: Вызываем функцию загрузки через StringIO
            load_via_copy_stringio(df_small_for_load, small_table_name)
            # Функция load_via_copy_stringio выведет сообщение об успехе/ошибке загрузки

````
Получаем результат
![image](https://github.com/user-attachments/assets/d3879bed-8223-4a83-936c-1099d15ef06b)

# Задание 3. Загрузка больших данных. Метод: copy_expert (file)
````
print(f"\n--- Задача 3: Загрузка данных из '{big_csv_path}' в '{big_table_name}' (метод file) ---")
    if os.path.exists(big_csv_path):
        load_via_copy_file(big_csv_path, big_table_name)
    else:
        print(f"ОШИБКА: Файл '{big_csv_path}' не найден. Загрузка не выполнена.")
````
Получаем результат
![image](https://github.com/user-attachments/assets/06784a09-bb57-4c3b-97ae-f8e26e897e2c)

# Задание 4. SQL Анализ	SQL: Вычислить средний cost в sales_big, где quantity = 50.
````
print("\n--- Задача 4: SQL Анализ таблицы sales_big ---")
    sql_query_task4 = f"""
    SELECT AVG(cost) as average_cost
        FROM sales_big
        WHERE quantity = 50;
    """
    # Выполняем запрос
    cursor.execute(sql_query_task4)
    result = cursor.fetchone()
        
    # Извлекаем результат
    avg_cost = result[0] if result else None
        
    # Выводим результат
    if avg_cost is not None:
            print(f"Средний cost в sales_big для quantity = 50: {avg_cost:.2f}")
    else:
            print("Нет данных, удовлетворяющих условию quantity = 50")
````
Получаем результат
![image](https://github.com/user-attachments/assets/297dac8e-9223-4be4-8726-c1540ce9f9a8)
# Вывод
В ходе данной работы мы определили наиболее эффективный метод загрузки данных (малых и больших объемов) из CSV-файлов в СУБД PostgreSQL и провели анализ данных.


