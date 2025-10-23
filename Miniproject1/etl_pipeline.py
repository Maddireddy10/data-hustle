import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# ------- EXTRACT --------
def extract(file_path):
    print("Extracting data...")
    df = pd.read_csv(file_path)
    print(f"Extracted {len(df)} rows.")
    return df

# ---------- TRANSFORM ------------
def transform(df):
    print("Transforming data...")
    text_cols = ['show_id', 'type', 'title', 'director', 'cast', 
                 'country', 'date_added', 'release_year', 'rating', 
                 'duration', 'listed_in', 'description']
    for col in text_cols:
        df[col] = df[col].astype(str).str.strip()
    df.replace('nan', np.nan, inplace=True)
    
    df['director'].fillna('Unknown', inplace=True)
    df['cast'].fillna('Unknown', inplace=True)
    df['country'].fillna('Unknown', inplace=True)
    df['rating'].fillna('Not rated', inplace=True)
    
    df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
    df['release_year'] = pd.to_numeric(df['release_year'], errors='coerce')
    
    df['duration_int'] = df['duration'].str.extract(r'(\d+)').astype(float)
    df['duration_type'] = df['duration'].str.extract(r'([a-zA-Z]+)')
    df['duration_int'].fillna(0, inplace=True)
    df['duration_type'].fillna('min', inplace=True)
    
    df['listed_in'] = df['listed_in'].str.replace(',', ' |')
    df.columns = [col.lower() for col in df.columns]
    df.drop(columns='duration', inplace=True)
    print("Transformation complete.")
    return df

# ------------ LOAD ---------------
def load(df):
    print("Loading data into PostgreSQL...")
    username = "postgres"
    password = "1234"
    host = "localhost"
    port = "5432"
    database = "netflix_project"

    engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')
    
    try:
        df.to_sql('netflix_titles', engine, if_exists='replace', index=False)
        print("Data loaded into PostgreSQL successfully!")
    except Exception as e:
        print("Load failed:", e)
    
    return engine

# -------------- QUERY ------------------
def query_data(engine):
    print("Running SQL queries...")
    try:
        with engine.connect() as connection:
            print("Connected to database for querying.")
            
            # Count total rows
            result = connection.execute(text("SELECT COUNT(*) FROM netflix_titles"))
            row_count = result.scalar()
            print(f"Total rows in table: {row_count}")

            # Sample rows
            result = connection.execute(text("SELECT title, type, country, rating FROM netflix_titles LIMIT 5"))
            rows = result.fetchall()
            print("\nSample records:")
            for row in rows:
                print(row)

    except Exception as e:
        print("Query failed:", e)

# ---------------- MAIN ----------------
def main():
    file_path = r"c:\Users\ilaas\OneDrive\Desktop\data-hustle\Miniproject1\Data\netflix_titles.csv"
    df = extract(file_path)
    df = transform(df)
    engine = load(df)
    query_data(engine)
    print("\nPipeline finished successfully!")
    print(df.head())

if __name__ == "__main__":
    main()



