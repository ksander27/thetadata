CREATE TABLE IF NOT EXISTS option_data (
    _dt_ DATE,
    _root_ TEXT,
    _ivl_ INTEGER,
    _exp_ DATE,
    _right_ TEXT,
    _strike_ INTEGER,
    _start_date_ DATE,
    _end_date_ DATE,
    _ms_of_day_ INTEGER,
    _midpoint_ FLOAT,
    _implied_vol_ FLOAT,
    _underlying_price_ FLOAT,
    _delta_ FLOAT,
    _bid_size_ INTEGER,
    _bid_condition_ INTEGER,
    _bid_ FLOAT,
    _ask_size_ INTEGER,
    _ask_condition_ INTEGER,
    _ask_ FLOAT,

    _url_ TEXT,
    _task_id_ TEXT,
    _req_id_ INTEGER,
    _latency_ms_ INTEGER,
    _err_type_ TEXT,


);
This statement creates a table named option_data with columns matching the CSV file columns you provided. You can modify the column data types as needed based on the actual data in your CSV file.

You can execute this CREATE TABLE statement using the psql client inside the container or any other method you prefer.





Regenerate response
