import pandas as pd
import numpy as np

def process_alcohol_data(path_to_data: str):
    """
    Loads alcohol sellers' data relevant to the assignment.
    """

    # Reading the csv file
    alcohol_data = pd.read_csv(path_to_data)

    # Checking for missing values
    if alcohol_data.isna().sum().sum() != 0:
        print("The dataset contains missing values.")
    
    # Counting the number of alcohol sellers in each voivodeship
    alcohol_seller_counts = alcohol_data['Województwo'].value_counts().reset_index()

    # Renaming columns
    alcohol_seller_counts.columns = ['Voivodeship', 'Number of sellers']

    # Changing voivodeships names to lowercase without 'woj. ' prefix
    alcohol_seller_counts['Voivodeship'] = alcohol_seller_counts['Voivodeship'].str.lower()
    alcohol_seller_counts['Voivodeship'] = alcohol_seller_counts['Voivodeship'].str.replace('woj. ', '', regex=False)
    return alcohol_seller_counts


def process_fire_data(path_to_data: str):
    """
    Loads fire events' data relevant to the assignment.
    """

    # Reading the csv file
    fire_data = pd.read_csv(path_to_data)

    # Only leaving in first 5 columns
    fire_data = fire_data.iloc[:, :5]

    # Checking for missing values
    if fire_data.isna().sum().sum() != 0:
        print("The dataset contains missing values.")
    
    # Changing names of columns
    fire_data.columns = ['TERYT', 'Voivodeship', 'Powiat', 'Gmina', 'Number of events']

    # Summing the number of events for each powiat
    fire_data = (
    fire_data[['Voivodeship', 'Powiat', 'Number of events']]
    .groupby(['Voivodeship', 'Powiat'], as_index=False)
    .sum()
    )

    return fire_data

def process_population_data(path_to_data: str):
    """
    Loads population data relevant to the assignment.
    """

    # Reading the file
    population_data = pd.read_excel(path_to_data, sheet_name = 2, skiprows = 3)

    # Only leaving columns 0,1 and 4
    population_data = population_data.iloc[:, [0,1, 4]]

    # Renaming columns
    population_data.columns = ['Voivodeship', 'Powiat', 'Population']

    population_data['Voivodeship'] = population_data['Voivodeship'].apply(
    lambda x: x if isinstance(x, str) and x.strip().startswith('WOJ.') else np.nan
    )

    # Forward filling the 'Voivodeship' column
    population_data['Voivodeship'] = population_data['Voivodeship'].ffill()
    # Removing 'WOJ. ' prefix from the 'Voivodeship' column

    # Dropping rows with NaNs
    population_data.dropna(inplace=True)

    # Removing prefix 'WOJ. ' from the 'Voivodeship' column
    population_data['Voivodeship'] = population_data['Voivodeship'].str.replace('WOJ. ', '', regex=False)

    # Changing voivodeship names to lowercase
    population_data['Voivodeship'] = population_data['Voivodeship'].str.lower()

    # Removing prefix '   ' from the 'Powiat' column
    population_data['Powiat'] = population_data['Powiat'].str.strip()

    # Removing prefix 'm. st. ' from 'm. st. Warszawa'
    population_data['Powiat'] = population_data['Powiat'].str.replace('m. st. ', '', regex=False)

    # Resetting the index
    population_data = population_data.reset_index(drop=True)

    return population_data


def process_area_data(path_to_data: str):
    """
    Loads area data relevant to the assignment.
    """

    # Reading the csv file
    area_data = pd.read_excel(path_to_data, skiprows=4)

    # Using only first 3 columns
    area_data = area_data.iloc[:, :3]

    # Renaming columns
    area_data.columns = ['Voivodeship', 'Powiat', 'Area (ha)']

    # Filling the 'Voivodeship' column
    area_data['Voivodeship'] = area_data['Powiat']
    area_data['Voivodeship'] = area_data['Voivodeship'].apply(
    lambda x: x if isinstance(x, str) and x.strip().startswith('WOJ.') else np.nan
    )
    area_data['Voivodeship'] = area_data['Voivodeship'].ffill()

    # Removing 'WOJ. ' prefix from the 'Voivodeship' column
    area_data['Voivodeship'] = area_data['Voivodeship'].str.replace('WOJ. ', '', regex=False)

    # Changing voivodeship names to lowercase
    area_data['Voivodeship'] = area_data['Voivodeship'].str.lower()

    # Removing non Powiat rows
    area_data['Powiat'] = area_data['Powiat'].apply(
    lambda x: x if isinstance(x, str) and x.strip().startswith('Powiat') else np.nan
    )
    area_data = area_data.dropna(subset=['Powiat'])
    # Removing prefix 'Powiat ' from the 'Powiat' column
    area_data['Powiat'] = area_data['Powiat'].str.replace('Powiat ', '', regex=False)
    # Removing prefix 'm. ' from the 'Powiat' column
    area_data['Powiat'] = area_data['Powiat'].str.replace('m. ', '', regex=False)
    # Removing prefix 'St. ' from the 'Powiat' column
    area_data['Powiat'] = area_data['Powiat'].str.replace('St. ', '', regex=False)

    # Resetting the index
    area_data = area_data.reset_index(drop=True)

    return area_data

def check_names_consistency(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    column_name: str
):
    """
    Checks if the names in the specified column of two DataFrames are consistent.
    """
    df1_names = set(df1[column_name])
    df2_names = set(df2[column_name])

    if df1_names != df2_names:
        print("Inconsistent names found:")
        print("In DataFrame 1 but not in DataFrame 2:")
        print(df1_names - df2_names)
        print("In DataFrame 2 but not in DataFrame 1:")
        print(df2_names - df1_names)
    else:
        print("All names are consistent.")

def merge_dataframes(list_of_dfs: list, mode: str):
    """
    Merges multiple DataFrames on specified columns (can be one or multiple).
    """

    assert mode in ['Voivodeship', 'Powiat']

    if mode == 'Powiat':
        merged_df = list_of_dfs[0]
        for df in list_of_dfs[1:]:
            merged_df = pd.merge(merged_df, df, on=['Voivodeship', 'Powiat'], how='outer')
        return merged_df.reset_index(drop=True)
    
    else:
        for i in range(len(list_of_dfs)):
            list_of_dfs[i] = list_of_dfs[i].groupby('Voivodeship', as_index=False).sum(numeric_only=True)
        merged_df = list_of_dfs[0]
        for df in list_of_dfs[1:]:
            merged_df = pd.merge(merged_df, df, on=['Voivodeship'], how='outer')
        return merged_df.reset_index(drop=True)


def calculate_basic_statistics(df: pd.DataFrame):
    """
    Calculates basic statistics for a specified column in a DataFrame.
    """
    column_names = df.columns
    retval = []

    for column in df.columns:
        if pd.api.types.is_numeric_dtype(df[column]):
            retval.append({
                'column': column,
                'mean': df[column].mean(),
                'median': df[column].median(),
                'std_dev': df[column].std(),
                'min': df[column].min(),
                'max': df[column].max()
            })
    
    return retval

def calculate_correlations(df: pd.DataFrame):
    """
    Calculates correlation coefficients between numeric columns in a DataFrame.
    """
    return df.corr().reset_index()