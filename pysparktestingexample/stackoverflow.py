from functools import reduce

def rename_chars(column_name):
    chars = ((' ', '_&'), ('.', '_$'))
    new_cols = reduce(lambda a, kv: a.replace(*kv), chars, column_name)
    return new_cols


def column_names(df):
    changed_col_names = df.schema.names
    for cols in changed_col_names:
        df = df.withColumnRenamed(cols, rename_chars(cols))
    return df
