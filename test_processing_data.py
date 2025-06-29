# tests/test_analysis.py
import pandas as pd
import numpy as np
from data_analysis_package.processing_data import calculate_basic_statistics
from data_analysis_package.processing_data import merge_dataframes
from data_analysis_package.processing_data import process_alcohol_data
from data_analysis_package.processing_data import process_fire_data
from data_analysis_package.processing_data import process_population_data
from data_analysis_package.processing_data import process_area_data
import tempfile
import os
import pytest

# Tests for calculate_basic_statistics()

def test_calculate_basic_statistics_basic_case():
    df = pd.DataFrame({
        'a': [1, 2, 3, 4, 5],
        'b': [10, 20, 30, 40, 50],
        'c': ['a', 'b', 'c', 'd', 'e']
    })

    result = calculate_basic_statistics(df)

    # Check that only numeric columns are processed
    assert set(result['column']) == {'a', 'b'}

    # Checking results
    a_stats = result[result['column'] == 'a'].iloc[0]
    assert a_stats['mean'] == 3
    assert a_stats['median'] == 3
    assert np.isclose(a_stats['std_dev'], 1.5811, atol=0.001)
    assert a_stats['min'] == 1
    assert a_stats['max'] == 5

    b_stats = result[result['column'] == 'b'].iloc[0]
    assert b_stats['mean'] == 30
    assert b_stats['median'] == 30
    assert np.isclose(b_stats['std_dev'], 15.811, atol=0.001)
    assert b_stats['min'] == 10
    assert b_stats['max'] == 50

# Empty df case
def test_calculate_basic_statistics_empty_df():
    df = pd.DataFrame()
    result = calculate_basic_statistics(df)
    assert result.empty

# Testing all non-numeric columns
def test_calculate_basic_statistics_all_non_numeric():
    df = pd.DataFrame({
        'x': ['a', 'b', 'c'],
        'y': ['foo', 'bar', 'baz']
    })
    result = calculate_basic_statistics(df)
    assert result.empty

# Testing with NaN values
def test_calculate_basic_statistics_nan_handling():
    df = pd.DataFrame({
        'num': [1, 2, np.nan, 4, 5]
    })
    result = calculate_basic_statistics(df)
    stats = result.loc[result['column'] =='num'].iloc[0]
    assert stats['mean'] == 3.0
    assert stats['median'] == 3.0
    assert stats['min'] == 1.0
    assert stats['max'] == 5.0


# Tests for merge_dataframes()
def test_merge_dataframes_mode_powiat():
    # Typical case with two dataframes
    df1 = pd.DataFrame({
        'Voivodeship': ['A', 'A', 'B', 'B', 'C'],
        'Powiat': [1, 2, 1, 2, 5],
        'b': [10, 20, 30, 40, 50],
    })
    df2 = pd.DataFrame({
        'Voivodeship': ['A', 'A', 'B', 'B', 'C'],
        'Powiat': [1, 2, 1, 2, 5],
        'c': [1, 2, 3, 4, 5]
    })

    intended_merged = pd.DataFrame({
        'Voivodeship': ['A', 'A', 'B', 'B', 'C'],
        'Powiat': [1, 2, 1, 2, 5],
        'b': [10, 20, 30, 40, 50],
        'c': [1, 2, 3, 4, 5]
    })

    merged_dataframes = merge_dataframes([df1, df2], mode='Powiat')
    assert merged_dataframes.equals(intended_merged)

    # Missing 'Powiat' in one dataframe
    df3 = df1.drop(columns='Powiat')
    with pytest.raises(KeyError):
        merge_dataframes([df1, df3], mode='Powiat')

    # Missing 'Voivodeship' in one dataframe
    df4 = df1.drop(columns='Voivodeship')
    with pytest.raises(KeyError):
        merge_dataframes([df1, df4], mode='Powiat')

def test_merge_dataframes_mode_voivodeship():
    # Typical case with two dataframes
    df1 = pd.DataFrame({
        'Voivodeship': ['A', 'A', 'B', 'B', 'C'],
        'b': [10, 20, 30, 40, 50],
    })
    df2 = pd.DataFrame({
        'Voivodeship': ['A', 'A', 'B', 'B', 'C'],
        'c': [1, 2, 3, 4, 5]
    })

    intended_merged = pd.DataFrame({
    'Voivodeship': ['A', 'B', 'C'],
    'b': [30, 70, 50],
    'c': [3, 7, 5]
    })

    merged_dataframes = merge_dataframes([df1, df2], mode='Voivodeship')
    assert merged_dataframes.equals(intended_merged)

    # Missing 'Voivodeship' in one dataframe
    df3 = df1.drop(columns='Voivodeship')
    with pytest.raises(KeyError):
        merge_dataframes([df1, df3], mode='Voivodeship')


# Mode not in ['Voivodeship', 'Powiat']
def test_merge_dataframes_mode_invalid():
    df1 = pd.DataFrame({
        'Voivodeship': ['A', 'A', 'B', 'B', 'C'],
        'Powiat': [1, 2, 1, 2, 5],
        'b': [10, 20, 30, 40, 50],
    })
    df2 = pd.DataFrame({
        'Voivodeship': ['A', 'A', 'B', 'B', 'C'],
        'Powiat': [1, 2, 1, 2, 5],
        'c': [1, 2, 3, 4, 5]
    })

    with pytest.raises(AssertionError):
        merge_dataframes([df1, df2], mode='InvalidMode')

# Empty list of DataFrames
def test_merge_dataframes_empty_list():

    # Empty list should raise an error
    with pytest.raises(IndexError):
        merge_dataframes([], mode='Powiat')
    with pytest.raises(IndexError):
        merge_dataframes([], mode='Voivodeship')

# One DataFrame in the list
def test_merge_dataframes_single_df():
    df = pd.DataFrame({
        'Voivodeship': ['A', 'B'],
        'Powiat': [1, 2],
        'val': [10, 20]
    })
    result = merge_dataframes([df], mode='Powiat')
    assert result.equals(df)

# Processing data files tests
# These functions are meant to be used with only one specific file each, so there's no need to test results

def test_process_alcohol_data(tmp_path):
    # Creating a temporary file from the first rows of data to simulate the input
    alcohol_data = pd.DataFrame({
    'Numer zezwolenia': ['335/22', '340/22', '345/22', '347/22', '352/22'],
    'Nazwa firmy': ['SŁAWOMIR OPIŁA  TOAST', 'PRZEDSIĘBIORSTWO WIELOBRANŻOWE    "BETA"   Spółka z ograniczoną odpowiedzialnością', 'ARHELAN  Spółka z ograniczoną odpowiedzialnością (dawniej: ARHELAN Helena Burzyńska Spółka komandytowa)', '"AMBRA"   SPÓŁKA AKCYJNA', '" JOTMAR"    Spółka z ograniczoną odpowiedzialnością'],
    'Kod pocztowy': ['27-200', '98-220', '17-100', '02-819', '64-510'],
    'Miejscowość': ['Starachowice', 'Zduńska Wola', 'Bielsk Podlaski', 'Warszawa', 'Wronki'],
    'Adres': ['ul. inż. Władysława Rogowskiego 7 ', 'ul. Opiesińska 30 A ', 'Aleja Józefa Piłsudskiego 45', 'ul. Puławska 336 ', 'ul. Mickiewicza  26 '],
    'Województwo': ['WOJ. ŚWIĘTOKRZYSKIE', 'WOJ. ŁÓDZKIE', 'WOJ. PODLASKIE', 'WOJ. MAZOWIECKIE', 'WOJ. WIELKOPOLSKIE'],
    'Data ważności': pd.to_datetime(['2024-01-03', '2024-01-03', '2024-01-14', '2024-01-03', '2024-01-03'])
    })

    # Creating a temporary file
    input_path = tmp_path / "input.csv"

    # Write input file
    alcohol_data.to_csv(input_path, index=False)

    # Proper use case
    processed_data = process_alcohol_data(path_to_data=str(input_path))

    # Checking if the DataFrame has no NaNs
    assert processed_data.isnull().sum().sum() == 0

    # Wrong path
    with pytest.raises(FileNotFoundError):
        process_alcohol_data(path_to_data="wrong_path.csv")
    
    # Checking if the shape of the DataFrame is correct
    assert processed_data.shape == (5, 2)

def test_process_fire_data(tmp_path):
    df = pd.DataFrame({
    "TERYT": [20101, 20102, 20103, 20104, 20105],
    "Województwo": ["dolnośląskie"]*5,
    "Powiat": ["bolesławiecki"]*5,
    "Gmina": ["Bolesławiec", "Bolesławiec", "Gromadka", "Nowogrodziec", "Osiecznica"],
    "OGÓŁEM Liczba zdarzeń": [79, 75, 66, 155, 41],
    "RAZEM 1. Obiekty użyteczności publicznej": [2, 1, 1, 0, 0],
    "101. Administracyjno-biurowe, banki": [0, 0, 0, 0, 0],
    # ... etc.
    })

    # Creating a temporary file
    
    input_path = tmp_path / "input.csv"

    # Write input file
    df.to_csv(input_path, index=False)

    # Proper use case
    processed_data = process_fire_data(path_to_data=input_path)

    # Checking if the DataFrame has no NaNs
    assert processed_data.isnull().sum().sum() == 0

    # Wrong path
    with pytest.raises(FileNotFoundError):
        process_fire_data(path_to_data="wrong_path.csv")

    # Checking if the shape of the DataFrame is correct
    assert processed_data.shape == (1, 3)

def test_process_population_data():
    # Only checking what happens when the path is wrong because the function requires a specific file structure
    with pytest.raises(FileNotFoundError):
        process_population_data(path_to_data="wrong_path.csv")

def test_process_area_data():
    # Only checking what happens when the path is wrong because the function requires a specific file structure
    with pytest.raises(FileNotFoundError):
        process_area_data(path_to_data="wrong_path.csv")
