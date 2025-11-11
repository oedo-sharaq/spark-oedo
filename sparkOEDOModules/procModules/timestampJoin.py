from pyspark.sql import DataFrame

def timestampJoin(df_left: DataFrame, df_right: DataFrame, left_ts_col: str, right_ts_col: str, join_type: str = 'inner', time_tolerance: int = 1000, time_offset: int = 0, on = []) -> DataFrame:
    """
    Perform a time-based join between two DataFrames on timestamp columns within a specified time tolerance.

    Parameters:
    df_left (DataFrame): The left DataFrame.
    df_right (DataFrame): The right DataFrame.
    left_ts_col (str): The name of the timestamp column in the left DataFrame.
    right_ts_col (str): The name of the timestamp column in the right DataFrame.
    join_type (str): Type of join to perform ('inner', 'left', 'right', 'outer').
    time_tolerance (int): The time tolerance for the join (in ts ticks).
    time_offset (int): An optional offset to apply to the right timestamp column before joining.
    on (list): Additional join conditions as a list of Column expressions.

    Returns:
    DataFrame: The resulting DataFrame after performing the time-based join.
    """
    from pyspark.sql import functions as F
    from pyspark.sql import Window

    # If there are identical column names in left and right DataFrames,
    # rename overlapping columns by adding left_/right_ prefixes to avoid
    # ambiguity after the join. Update timestamp column names if they are renamed.
    left_cols = set(df_left.columns)
    right_cols = set(df_right.columns)
    common_cols = left_cols.intersection(right_cols)
    for col in common_cols:
        # skip if already prefixed
        if col.startswith("left_") or col.startswith("right_"):
            continue
        new_left = f"left_{col}"
        new_right = f"right_{col}"
        df_left = df_left.withColumnRenamed(col, new_left)
        df_right = df_right.withColumnRenamed(col, new_right)
        if col == left_ts_col:
            left_ts_col = new_left
        if col == right_ts_col:
            right_ts_col = new_right

    # Apply time offset to the right timestamp column if specified
    if time_offset != 0:
        df_right = df_right.withColumn(right_ts_col, (df_right[right_ts_col] + F.lit(time_offset)))

    # Create a new column in df_left and df_right to represent the floored timestamp
    FLOORING_FACTOR = 10 * time_tolerance # Define a flooring factor to group timestamps
    # Use qualified column references (df_left[...] and df_right[...]) so the same
    # column name can exist in both DataFrames without ambiguity.
    df_left = df_left.withColumn("left_ts_floor", F.floor((df_left[left_ts_col] / FLOORING_FACTOR)).cast("long"))
    df_right = df_right.withColumn("right_ts_floor", F.floor((df_right[right_ts_col] / FLOORING_FACTOR)).cast("long"))

    # Build the time-difference condition using qualified columns to avoid
    # accidental name collisions when left_ts_col == right_ts_col.
    condition = [
        (F.abs(df_left[left_ts_col] - df_right[right_ts_col]) <= time_tolerance)
    ]

    # Perform the join on the floored timestamp columns and the additional conditions
    condition0 = condition + [F.col("left_ts_floor") == F.col("right_ts_floor")] + on
    df_joined = df_left.join(df_right, condition0, join_type)

    # Also join on neighboring floored timestamps to account for boundary cases
    condition1 = condition + [F.col("left_ts_floor") == F.col("right_ts_floor") + 1] + on
    df_joined = df_joined.union(df_left.join(df_right, condition1, join_type))

    condition2 = condition + [F.col("left_ts_floor") == F.col("right_ts_floor") - 1] + on
    df_joined = df_joined.union(df_left.join(df_right, condition2, join_type))

    # Drop the temporary floored timestamp columns
    joined_df = df_joined.drop("left_ts_floor", "right_ts_floor")

    return joined_df