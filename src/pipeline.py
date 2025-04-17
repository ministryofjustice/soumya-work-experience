"""Loading both tables of data into duckDB with custom transformers.

Here we use custom transformers to modify existing data, combine fields
and remove columns.
"""
from pathlib import Path

import dlt
from dlt.sources.filesystem import filesystem, read_jsonl


def redact(
    row: dict,
    columns: list[str],
) -> dict:
    """Redact data in the specified columns from the row."""
    for column in columns:
        row[column] = "***"
    return row


def convert_to_datetime(
    row: dict,
) -> dict:
    """Create a datetime string from separate date and times.

    Combines event_date and event_time fields into a single event_timestamp value
    before dropping the original fields.
    """
    row["event_timestamp"] = f"{row['event_date']} {row['event_time']}"
    del row["event_date"]
    del row["event_time"]
    return row


def load_data(pipeline_name: str) -> str:
    """Run dlt pipeline to load data."""
    current_directory = Path().cwd()

    files_users = filesystem(
        bucket_url=f"file://{current_directory}/data",
        file_glob="users_*.jsonl",
    )
    columns_to_redact = ["name", "alias", "email", "phone_number", "address"]
    reader_users = (
        (files_users | read_jsonl()\
        .add_map(lambda row: redact(row, columns_to_redact)))\
        .with_name("users")
    )

    files_events = filesystem(
        bucket_url=f"file://{current_directory}/data",
        file_glob="events_*.jsonl",
    )

    reader_events = (
        (files_events | read_jsonl())\
        .add_map(lambda row: convert_to_datetime(row))\
        .with_name("events")
    )

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="duckdb",
        dataset_name="workshop_data",
        dev_mode=True,
    )

    load_info = pipeline.run([reader_users, reader_events])

    print(load_info)  # noqa: T201

    return load_info.dataset_name


if __name__ == "__main__":
    import duckdb

    pipeline_name = "example_6"
    database = load_data(pipeline_name)
    db_file = f"{pipeline_name}.duckdb"

    print("""
    We no longer have a seperate users__alias table
    """)  # noqa: T201
    with duckdb.connect(f"{db_file}") as conn:
        print(conn.sql(f"""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = '{database}';
        """))  # noqa: T201

    print("""
    We see that the users data looks just the same as example 1 but heavily redacted.
    """)  # noqa: T201
    with duckdb.connect(f"{db_file}") as conn:
        query = conn.sql(f"""
        select *
        from {database}.users
        limit 10
        """)
        query.show()

    print("""
    And the events date and time are now combined into one field.
    """)  # noqa: T201
    with duckdb.connect(f"{db_file}") as conn:
        query = conn.sql(f"""
        select *
        from {database}.events
        limit 10
        """)
        query.show()
