import pandas as pd
import numpy as np

def extract(file_path):
    df = pd.read_csv(file_path)
    return df

def transform(df):
    # Text cleaning
    text_cols = ['show_id', 'type', 'title', 'director', 'cast', 
                 'country', 'date_added', 'release_year', 'rating', 
                 'duration', 'listed_in', 'description']
    for col in text_cols:
        df[col] = df[col].astype(str).str.strip()
    df.replace('nan', np.nan, inplace=True)
    
    # Fill missing values
    df['director'].fillna('Unknown', inplace=True)
    df['cast'].fillna('Unknown', inplace=True)
    df['country'].fillna('Unknown', inplace=True)
    df['rating'].fillna('Not rated', inplace=True)
    
    # Dates and numeric
    df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
    df['release_year'] = pd.to_numeric(df['release_year'], errors='coerce')
    
    # Duration split
    df['duration_int'] = df['duration'].str.extract(r'(\d+)').astype(float)
    df['duration_type'] = df['duration'].str.extract(r'([a-zA-Z]+)')
    df['duration_int'].fillna(0, inplace=True)
    df['duration_type'].fillna('min', inplace=True)
    
    # Genre formatting
    df['listed_in'] = df['listed_in'].str.replace(',',' |')
    
    # Lowercase columns
    df.columns = [col.lower() for col in df.columns]
    
    # Drop original duration
    df.drop(columns='duration', inplace=True)
    
    return df

def main():
    file_path = r"c:\Users\ilaas\OneDrive\Desktop\data-hustle\Miniproject1\Data\netflix_titles.csv"
    df = extract(file_path)
    df = transform(df)
    
    # For now, just check data
    print(df.head())

if __name__ == "__main__":
    main()
