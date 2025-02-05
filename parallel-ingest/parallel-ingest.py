import psycopg2
from psycopg2.extras import execute_values
from multiprocessing import Pool
import time
import threading
import subprocess
import pandas as pd
from pandas import DataFrame
import logging
import logging.handlers as handlers
import time
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
import resource
import math

# Database connection configuration
DB_HIS_CONFIG = {
    'dbname': 'captivecoredb_history',
    'user': 'matija',
    'password': 'stellar123',
    'host': 'localhost',
    'port': 5432
}

DB_FILTERED_CONFIG = {
    'dbname': 'captivecoredb_fil',
    'user': 'matija',
    'password': 'stellar123',
    'host': 'localhost',
    'port': 5432
}

# Define batch size
PARALLEL_INGEST_COMMAND = "stellar-horizon db reingest range"
BATCH_SIZE = 1000
CHUNK_SIZE = 1000
#ITERATION_SIZE = 2000
NUM_PROCESSES = 4

#set max memory
MEMORY_TO_USE = 4 # numebr of gb 
MAX_MEMORY = MEMORY_TO_USE * 1024 * 1024 * 1024 # convert to gb 

logger = logging.getLogger(__name__)
logname = "logs/ledger-ingestion-deletion.log"
handler = TimedRotatingFileHandler(logname, when="midnight", backupCount=30)
handler.suffix = "%Y%m%d"
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', "%Y-%m-%d %H:%M:%S")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)



#limit memory 
def set_memory_limit():
    resource.setrlimit(resource.RLIMIT_AS, (MAX_MEMORY, MAX_MEMORY))
    logger.info(f"Memory usage limited to {MEMORY_TO_USE} GB")


# Count number of ledgers in the last 30 days
def count_one_month_ledgers(conn):
    ledger_count_one_month = 0
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM history_ledgers WHERE closed_at >= CURRENT_DATE - INTERVAL '30 days';")
        ledger_count_one_month = cursor.fetchone()[0]
    print(f"Ledger count {ledger_count_one_month}")
    logger.info(f"Ledger count {ledger_count_one_month}.")
    return ledger_count_one_month


def get_first_ledger(conn):
    latest_ledger = 0
    with conn.cursor() as cursor:
        cursor.execute("SELECT MIN(sequence) FROM history_ledgers;")
        latest_ledger = cursor.fetchone()[0]
    print(f"Latest ledger {latest_ledger}")
    logger.info(f"Latest ledger {latest_ledger}.")
    return latest_ledger


# def create_chunks(start: int, end: int):
#     return [(i, min(i + CHUNK_SIZE - 1, end)) 
#             for i in range(start, end + 1, CHUNK_SIZE)]

def create_chunks(start: int, end: int):
    """
    Generator that yields (start, end) tuples for chunks.
    """
    for i in range(start, end + 1, CHUNK_SIZE):
        chunk_created = min(i + CHUNK_SIZE - 1, end)
        print(i, chunk_created)
        yield (i, chunk_created)


# Run the parallel ingest command for a given ledger range.
# def parallel_ingest(chunk):
#     start, end = chunk
#     try:
#         ingest_command = f"{PARALLEL_INGEST_COMMAND} {start} {end}"
#         logger.info(f"Running ingestion: {ingest_command}")
#         subprocess.run(ingest_command, shell=True, capture_output=True, text=True)
#         logger.info(f"Ingestion from ledgers {start} to {end} completed.")
#         return (start, end, True)
#     except subprocess.CalledProcessError as e:
#         logger.error(f"Ingestion failed for ledgers {start} to {end}: {e.stderr}")
#         return (start, end, False)


def parallel_ingest(chunk):
    start, end = chunk
    ingest_command = f"{PARALLEL_INGEST_COMMAND} {start} {end}"
    logger.info("Running ingestion: %s", ingest_command)
    try:
        # Avoid capturing output; send stdout and stderr to DEVNULL to keep memory usage down
        subprocess.run(
            ingest_command,
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True
        )
        logger.info("Ingestion from ledgers %s to %s completed.", start, end)
        return (start, end, True)
    except subprocess.CalledProcessError:
        logger.error("Ingestion failed for ledgers %s to %s.", start, end)
        return (start, end, False)

    

# Check if the data in the filtered DB matches
def check_data_in_filtered_db(conn_history, conn_filtered, start_ledger, end_ledger, max_retries, initial_wait):
    retries = 0
    wait_time = initial_wait

    while retries < max_retries:
        with conn_filtered.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM ledgers WHERE sequence BETWEEN %s AND %s;", (start_ledger, end_ledger))
            ledger_fil_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM transactions WHERE ledger_sequence BETWEEN %s AND %s;", (start_ledger, end_ledger))
            transaction_fil_count = cursor.fetchone()[0]

        with conn_history.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM history_ledgers WHERE sequence BETWEEN %s AND %s;", (start_ledger, end_ledger))
            ledger_his_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM history_transactions WHERE ledger_sequence BETWEEN %s AND %s;", (start_ledger, end_ledger))
            transaction_his_count = cursor.fetchone()[0]

        missing_ledgers = ledger_his_count - ledger_fil_count
        missing_transactions = transaction_his_count - transaction_fil_count

        if missing_ledgers == 0 and missing_transactions == 0:
            # Data ingestion verified
            logger.info(f"Data ingestion verified for ledgers {start_ledger} to {end_ledger}.")
            return True

        logger.warning(f"Data missing in filtered DB. Retrying in {wait_time} seconds...")
        time.sleep(wait_time)
        wait_time *= 2  # Exponential backoff
        retries += 1

    logger.error(f"Data not fully copied after {max_retries} retries. Skipping deletion for ledgers {start_ledger} to {end_ledger}.")
    return False

# Search number of records in ledgers and transactions tables between start and end ledgers. If same, break loop
# If not equal, for whichever unequal,   
def insert_missing_records(conn_history, conn_filtered, start_ledger, end_ledger):
    db_his_cursor = conn_history.cursor()
    db_fil_cursor = conn_filtered.cursor()

    # Counts
    db_fil_cursor.execute("SELECT COUNT(*) FROM ledgers WHERE sequence BETWEEN %s AND %s;", (start_ledger, end_ledger))
    ledger_fil_count = db_fil_cursor.fetchone()[0]
    db_fil_cursor.execute("SELECT COUNT(*) FROM transactions WHERE ledger_sequence BETWEEN %s AND %s;", (start_ledger, end_ledger))
    transaction_fil_count = db_fil_cursor.fetchone()[0]

    db_his_cursor.execute("SELECT COUNT(*) FROM history_ledgers WHERE sequence BETWEEN %s AND %s;", (start_ledger, end_ledger))
    ledger_his_count = db_his_cursor.fetchone()[0]
    db_his_cursor.execute("SELECT COUNT(*) FROM history_transactions WHERE ledger_sequence BETWEEN %s AND %s;", (start_ledger, end_ledger))
    transaction_his_count = db_his_cursor.fetchone()[0]

    if ledger_his_count == ledger_fil_count and transaction_his_count == transaction_fil_count:
        logger.info("Ledger and Transaction counts are the same. No insertion needed.")
        return

    # Step 4: Identify and insert missing ledgers
    db_fil_cursor.execute("SELECT sequence FROM ledgers WHERE sequence BETWEEN %s AND %s;", (start_ledger, end_ledger))
    existing_ledgers = {row[0] for row in db_fil_cursor.fetchall()}

    db_his_cursor.execute("SELECT * FROM history_ledgers WHERE sequence BETWEEN %s AND %s;", (start_ledger, end_ledger))

    missing_ledgers = []
    for row in db_his_cursor.fetchall():
        if row[0] not in existing_ledgers:
            missing_ledgers.append(row)

    if missing_ledgers:
        # Note: adjust columns/structure as needed for your 'ledgers' table
        db_fil_cursor.executemany(
            """
            INSERT INTO ledgers (sequence, data_column_1, data_column_2)
            VALUES (%s, %s, %s);
            """,
            missing_ledgers
        )
        logger.info(f"Inserted {len(missing_ledgers)} missing ledger records.")

    # Step 5: Identify and insert missing transactions
    db_fil_cursor.execute("SELECT id FROM transactions WHERE ledger_sequence BETWEEN %s AND %s;", (start_ledger, end_ledger))
    existing_transactions = {row[0] for row in db_fil_cursor.fetchall()}

    db_his_cursor.execute("SELECT * FROM history_transactions WHERE ledger_sequence BETWEEN %s AND %s;", (start_ledger, end_ledger))

    missing_transactions = []
    for row in db_his_cursor.fetchall():
        if row[0] not in existing_transactions:
            missing_transactions.append(row)

    if missing_transactions:
        # Note: adjust columns/structure as needed for your 'transactions' table
        db_fil_cursor.executemany(
            """
            INSERT INTO transactions (id, ledger_sequence, data_column_1, data_column_2)
            VALUES (%s, %s, %s, %s);
            """,
            missing_transactions
        )
        logger.info(f"Inserted {len(missing_transactions)} missing transaction records.")

    logger.info("Insertion of missing records done.")


## Maybe stop kafka and ingestion before truncate??????
def truncate_table(conn):
    with conn.cursor() as cursor:
        # Truncate  tables
        #cursor.execute(f"TRUNCATE TABLE {table_name};")
        cursor.execute("""TRUNCATE TABLE history_effects, history_operations, history_transactions, history_ledgers, history_operation_participants, 
                       history_transaction_participants, history_trades, history_claimable_balances, history_operation_claimable_balances, 
                       history_transaction_claimable_balances, history_trades_60000, history_operation_liquidity_pools, history_transaction_liquidity_pools, 
                       history_accounts, history_liquidity_pools, history_assets CASCADE;""")
    conn.commit()
    logger.info(f"Truncated tables completely.")

def print_db_size(conn, dbname):
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT pg_size_pretty(pg_database_size(%s))", (dbname,))
            db_size = cursor.fetchone()[0]
            print(f"Database {dbname} Size before truncation: {db_size}")
            logger.info(f"Database {dbname} Size before truncation: {db_size}")
    except Exception as e:
        logger.error(f"Error retrieving {dbname} size: {e}")


def main():
    # Set memory limit 
    #set_memory_limit()

    conn_history = None
    conn_filtered = None
    try:
        # Connect to databases
        conn_history = psycopg2.connect(**DB_HIS_CONFIG)
        conn_filtered = psycopg2.connect(**DB_FILTERED_CONFIG)

        loop_count = 0

        end_ledger = 54118600    # always end with 0 
        start_ledger =  54114600  # always end with 1

        total_iterations = math.ceil((end_ledger - (start_ledger))/(CHUNK_SIZE * NUM_PROCESSES))
        
        # descending by ledger sequence
        while end_ledger > start_ledger:
            loop_count += 1
            ITERATION_SIZE = CHUNK_SIZE * NUM_PROCESSES
            chunk_start = max(end_ledger - ITERATION_SIZE + 1, start_ledger)
            print(chunk_start, end_ledger)
            
            # chunks = create_chunks(chunk_start, end_ledger)
            # logger.info(f"Iteration {loop_count} of {total_iterations}: Processing ledgers from {chunk_start} to {end_ledger}")

            chunks = list(create_chunks(chunk_start, end_ledger))
            logger.info("Iteration %s of %.2f: Processing ledgers from %s to %s", loop_count, total_iterations, chunk_start, end_ledger)

            # Step 1: Run parallel ingestion
            logger.info(f"Starting step 1: Parallel ingestion for ledgers {chunk_start} to {end_ledger}")
            #parallel_ingest(chunk_start, end_ledger)
            logger.info(f"Starting ingestion of {len(chunks)} chunks")

            # with Pool(NUM_PROCESSES) as pool:
            #     ingestion_results = pool.map(parallel_ingest, chunks)

            # # Check for failed ingestions
            # if any(not result[2] for result in ingestion_results):
            #     logger.error("Aborting due to failed ingestions")
            #     return

            ingestion_failed = False
            with Pool(NUM_PROCESSES) as pool:
                for result in pool.imap_unordered(parallel_ingest, chunks):
                    if not result[2]:
                        ingestion_failed = True
                        break

            if ingestion_failed:
                logger.error("Aborting due to failed ingestions")
                break

            logger.info("Finished parallel ingestion for ledgers %s to %s", chunk_start, end_ledger)
                
            logger.info(f"Finished step 1: Parallel ingestion for ledgers {chunk_start} to {end_ledger}")

            # Step 2: Verify data in filtered database
            logger.info(f"Starting step 2: Data verification for ledgers {chunk_start} to {end_ledger}")

            # Tables to Truncate 
            ''' tables = [
                "history_effects", "history_operations", "history_transactions", "history_ledgers",
                "history_operation_participants", "history_transaction_participants", "history_trades",
                "history_claimable_balances", "history_operation_claimable_balances", "history_transaction_claimable_balances",
                "history_trades_60000", "history_operation_liquidity_pools", "history_transaction_liquidity_pools",
                "history_accounts", "history_liquidity_pools", "history_assets"
            ]
            '''

            if check_data_in_filtered_db(conn_history, conn_filtered, chunk_start, end_ledger, 5, 60):
                logger.info(f"Finished step 2: Data verification for ledgers {chunk_start} to {end_ledger} in filtered DB complete.")

                # Step 3: Check db size 

                print_db_size(conn_history, DB_HIS_CONFIG['dbname'])

                # Step 4: Truncate data in history database
                logger.info(f"Starting step 3: Truncating data in history database for entire tables (instead of batch deletion).")

                truncate_table(conn_history)

                logger.info(f"Finished step 3: Table truncation in history database completed.")
            else:
                # Step 5: Insert missing records and then truncate
                logger.warning(f"Starting step 4: Some data for ledgers {chunk_start} to {end_ledger} not found in filtered DB.")
                insert_missing_records(conn_history, conn_filtered, chunk_start, end_ledger)
                logger.info(f"Missing records for ledgers {chunk_start} to {end_ledger} inserted. Proceeding to truncate.")

                truncate_table(conn_history)

                logger.info(f"Finished step 4: Table truncation in history database completed.")

            conn_history.commit()
            conn_filtered.commit()
            
            # next chunk
            end_ledger = chunk_start - 1
            if end_ledger < start_ledger:
                logger.info("Chunk start >= chunk end. No more valid ranges to process. Stopping.")
                break

        print(f"Total Loop count is {loop_count}")

    except Exception as e:
        print(f"Error: {e}")
        if conn_history:
            conn_history.rollback()
            print("Rolledback history DB")
            logger.info("Rolledback history DB")
        if conn_filtered:
            conn_filtered.rollback()
            print("Rolledback filtered DB")
            logger.info("Rolledback filtered DB")
    finally:
        if conn_history:
            conn_history.close()
            print("History DB connection closed")
            logger.info("History DB connection closed")
        if conn_filtered:
            conn_filtered.close()
            print("Filtered DB connection closed")
            logger.info("Filtered DB connection closed")


if __name__ == "__main__":
    main()
