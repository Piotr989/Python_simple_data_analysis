import pandas as pd
import numpy as np
import argparse
from . import processing_data
import os

def run_analysis(input_path, output_path):

    # Processing paths
    alcohol_path = os.path.join(input_path, 'alkohol2024.csv')
    fire_path = os.path.join(input_path, 'pozary2024.csv')
    area_path = os.path.join(input_path, 'powierzchnia_geodezyjna2024.xlsx')
    population_path = os.path.join(input_path, 'powierzchnia_i_ludnosc2024.xlsx')

    # Processing data
    alcohol_data = processing_data.process_alcohol_data(path_to_data = alcohol_path)
    fire_data = processing_data.process_fire_data(path_to_data = fire_path)
    area_data = processing_data.process_area_data(path_to_data = area_path)
    population_data = processing_data.process_population_data(path_to_data = population_path)

    # Data sets for powiat and voivodeships
    powiat_data_sets = [fire_data, population_data, area_data]
    voivodeship_data_sets = [fire_data, population_data, area_data, alcohol_data]
    powiat_data_sets_names = ['fire_data', 'population_data', 'area_data'] # no alcohol sellers'data (too hard!)
    voivodeship_data_sets_names = ['fire_data', 'population_data', 'area_data', 'alcohol_data']

    data_by_powiat = processing_data.merge_dataframes(list_of_dfs = powiat_data_sets, mode = 'Powiat')
    data_by_voivodeship = processing_data.merge_dataframes(list_of_dfs = voivodeship_data_sets, mode = 'Voivodeship')

    # Calculating statistics and correlations by powiat and voivodeship
    statistics_by_powiat = processing_data.calculate_basic_statistics(data_by_powiat)
    statistics_by_voivodeship = processing_data.calculate_basic_statistics(data_by_voivodeship)
    correlations_by_powiat = data_by_powiat.iloc[:,2:].corr()
    correlations_by_voivodeship = data_by_voivodeship.iloc[:,1:].corr()

    # Saving results to csv files
    statistics_by_powiat.to_csv(os.path.join(output_path, 'statistics_by_powiat.csv'), index=False)
    statistics_by_voivodeship.to_csv(os.path.join(output_path, 'statistics_by_voivodeship.csv'), index=False)
    correlations_by_powiat.to_csv(os.path.join(output_path, 'correlations_by_powiat.csv'))
    correlations_by_voivodeship.to_csv(os.path.join(output_path, 'correlations_by_voivodeship.csv'))
    

if __name__ == "__main__":

    #Using argparse to handle command line arguments
    parser = argparse.ArgumentParser(description = "Runs data analysis according to the assignment.")

    parser.add_argument('--input', type=str, required=True, help='Path to the directory containing 4 files:' \
    'alkohol2024.csv, powierzchnia_geodezyjna2024.xlsx, powierzchnia_i_ludnosc2024.xlsx, pozary2024.csv')

    parser.add_argument('--output', type=str, required=True, help='Path to the directory where the output files will be saved.')

    args = parser.parse_args()

    run_analysis(args.input, args.output)

# Saving paths for ease of use:
"""
# --input D:\\user\\Documents\\GitHub\\Python_simple_data_analysis\\data_analysis_package\\data
# --output D:\\user\\Documents\\GitHub\\Python_simple_data_analysis\\output
"""