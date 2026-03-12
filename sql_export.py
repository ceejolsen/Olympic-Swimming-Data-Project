"""
sql_export.py
-------------
Utilities for exporting cleaned swimming race data to SQLite
and running example analytical queries.
"""

import sqlite3
import pandas as pd
import os

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "swim_results.db")


def time_to_seconds(t):
    """Convert swimming time strings like mm:ss.xx to total seconds."""
    if pd.isna(t):
        return None
    if isinstance(t, (int, float)):
        return float(t)

    t = str(t).strip()
    if ":" in t:
        m, s = t.split(":")
        return int(m) * 60 + float(s)

    return pd.to_numeric(t, errors="coerce")


def connect_db(db_path=DB_PATH):
    """
    Create and return a SQLite database connection.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        sqlite3.Connection: Active database connection.
    """
    return sqlite3.connect(db_path)


def prepare_for_sql(men_df, women_df):
    """
    Combine men's and women's data into one SQL-ready dataframe.

    Adds a sex column and converts key timing columns to numeric.

    Args:
        men_df (pd.DataFrame): Parsed men's race data.
        women_df (pd.DataFrame): Parsed women's race data.

    Returns:
        pd.DataFrame: Combined dataframe ready for SQLite export.
    """
    men = men_df.copy()
    women = women_df.copy()

    men["sex"] = "M"
    women["sex"] = "F"

    df = pd.concat([men, women], ignore_index=True)

    numeric_cols = [
        "reaction_time",
        "split_50m",
        "split_100m",
        "split_150m",
        "split_200m",
        "split_250m",
        "split_300m",
        "split_350m",
        "final_time",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(time_to_seconds)

    return df


def export_to_sqlite(df, db_path=DB_PATH, table_name="swim_results"):
    """
    Export a dataframe to a SQLite table.

    If the table already exists, it is replaced.

    Args:
        df (pd.DataFrame): Data to export.
        db_path (str): Path to the SQLite database file.
        table_name (str): Name of the SQL table.
    """
    conn = connect_db(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()


def get_table_schema(db_path=DB_PATH, table_name="swim_results"):
    """
    Return schema information for a SQLite table.

    Args:
        db_path (str): Path to the SQLite database file.
        table_name (str): Name of the SQL table.

    Returns:
        pd.DataFrame: Table schema info from PRAGMA table_info.
    """
    conn = connect_db(db_path)
    schema_df = pd.read_sql_query(f"PRAGMA table_info({table_name})", conn)
    conn.close()
    return schema_df


def count_rows(db_path=DB_PATH, table_name="swim_results"):
    """
    Count the total number of rows in a SQLite table.

    Args:
        db_path (str): Path to the SQLite database file.
        table_name (str): Name of the SQL table.

    Returns:
        int: Number of rows in the table.
    """
    conn = connect_db(db_path)
    query = f"SELECT COUNT(*) AS row_count FROM {table_name}"
    row_count = pd.read_sql_query(query, conn).iloc[0]["row_count"]
    conn.close()
    return int(row_count)


def query_to_df(query, db_path=DB_PATH):
    """
    Run a SQL query and return the result as a pandas dataframe.

    Args:
        query (str): SQL query string.
        db_path (str): Path to the SQLite database file.

    Returns:
        pd.DataFrame: Query result.
    """
    conn = connect_db(db_path)
    result_df = pd.read_sql_query(query, conn)
    conn.close()
    return result_df


def create_indexes(db_path=DB_PATH, table_name="swim_results"):
    """
    Create indexes to speed up common queries.

    Indexes are created on columns commonly used for filtering,
    sorting, or grouping.

    Args:
        db_path (str): Path to the SQLite database file.
        table_name (str): Name of the SQL table.
    """
    conn = connect_db(db_path)
    cursor = conn.cursor()

    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_sex "
        f"ON {table_name}(sex)"
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_final_time "
        f"ON {table_name}(final_time)"
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_last_name "
        f"ON {table_name}(last_name)"
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_link "
        f"ON {table_name}(Link)"
    )

    conn.commit()
    conn.close()


def run_sample_queries(db_path=DB_PATH, table_name="swim_results"):
    """
    Run example SQL queries and print their results.

    Args:
        db_path (str): Path to the SQLite database file.
        table_name (str): Name of the SQL table.

    Returns:
        dict[str, pd.DataFrame]: Dictionary of query titles to results.
    """
    queries = {
        "Top 10 fastest swims": f"""
            SELECT first_name, last_name, sex, final_time
            FROM {table_name}
            WHERE final_time IS NOT NULL
            ORDER BY final_time ASC
            LIMIT 10
        """,
        "Average final time by sex": f"""
            SELECT sex, ROUND(AVG(final_time), 2) AS avg_final_time
            FROM {table_name}
            WHERE final_time IS NOT NULL
            GROUP BY sex
        """,
        "Average reaction time by sex": f"""
            SELECT sex, ROUND(AVG(reaction_time), 3) AS avg_reaction_time
            FROM {table_name}
            WHERE reaction_time IS NOT NULL
            GROUP BY sex
        """,
        "Rows missing late splits": f"""
            SELECT COUNT(*) AS missing_late_splits
            FROM {table_name}
            WHERE split_300m IS NULL OR split_350m IS NULL
        """,
        "Swimmers parsed per race": f"""
            SELECT Link, COUNT(*) AS swimmer_count
            FROM {table_name}
            GROUP BY Link
            ORDER BY swimmer_count DESC
            LIMIT 10
        """,
        "Fastest swim per race": f"""
            SELECT Link, MIN(final_time) AS fastest_time
            FROM {table_name}
            WHERE final_time IS NOT NULL
            GROUP BY Link
            ORDER BY fastest_time ASC
            LIMIT 10
        """,
    }

    results = {}

    for title, query in queries.items():
        print(f"\n--- {title} ---")
        result_df = query_to_df(query, db_path)
        print(result_df)
        results[title] = result_df

    return results

def swimmers_per_race(db_path=DB_PATH, table_name="swim_results"):
    """
    Return the number of swimmer rows parsed from each PDF link.
    """
    query = f"""
        SELECT Link, COUNT(*) AS swimmer_count
        FROM {table_name}
        GROUP BY Link
        ORDER BY swimmer_count DESC
    """
    return query_to_df(query, db_path)


def fastest_swim_per_race(db_path=DB_PATH, table_name="swim_results"):
    """
    Return the fastest final time found in each race PDF.
    """
    query = f"""
        SELECT Link, MIN(final_time) AS fastest_time
        FROM {table_name}
        WHERE final_time IS NOT NULL
        GROUP BY Link
        ORDER BY fastest_time ASC
    """
    return query_to_df(query, db_path)


def average_final_time_per_race(db_path=DB_PATH, table_name="swim_results"):
    """
    Return the average final time for each race PDF.
    """
    query = f"""
        SELECT Link, ROUND(AVG(final_time), 2) AS avg_final_time
        FROM {table_name}
        WHERE final_time IS NOT NULL
        GROUP BY Link
        ORDER BY avg_final_time ASC
    """
    return query_to_df(query, db_path)