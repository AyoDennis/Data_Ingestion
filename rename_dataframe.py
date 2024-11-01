def rename_columns(df, new_names):
  """
  This function is for renaming columns in a pandas DataFrame.

  Args:
    df: The pandas DataFrame to rename columns in.
    new_names: A dictionary mapping old column names to new column names in the format below.

    new_names = 
        
  Returns:
    The DataFrame with renamed columns.
  """

  df = df.rename(columns=new_names)
  return df