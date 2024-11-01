def rename_columns(df, new_names):
    """
    This function is for renaming columns in a pandas DataFrame.
    Args:
        df: The pandas DataFrame to rename columns in.
        new_names: A dictionary mapping old to new column names in this format.

        new_names =
        {
            'old_column_1': 'new_column_1',
            'old_column_2': 'new_column_2',
        # ...
        }


    Returns:
        The DataFrame with renamed columns.
     """
    df = df.rename(columns=new_names)
    return df
