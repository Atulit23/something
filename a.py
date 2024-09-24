import sqlalchemy as sal
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Date, Float, text
import pandas as pd

# Database connection URL
db_url = 'postgresql://netflix_data_cleaning_gvro_user:Vbxny2At2GGehYt1v2ADitV8WUAcs55q@dpg-crpb8cij1k6c73c33890-a.oregon-postgres.render.com/netflix_data_cleaning_gvro'
engine = create_engine(db_url)
meta = MetaData()

# Function to load data from CSV into postgres
def load_data_to_postgres(csv_file):
    """Load data from a CSV file into the netflix_raw table in PostgreSQL."""
    # Read the CSV file
    df = pd.read_csv(csv_file)
    
    with engine.connect() as conn:
        df.to_sql('netflix_raw', con=conn, index=False, if_exists='append')

    print("Data loaded into 'netflix_raw' table.")

# 1. Function to create the tables
def create_tables():
    # Netflix Raw table
    netflix_raw = Table('netflix_raw', meta,
                        Column('show_id', String, primary_key=True),
                        Column('title', String),
                        Column('type', String),
                        Column('date_added', String),
                        Column('release_year', Integer),
                        Column('rating', String),
                        Column('duration', String),
                        Column('description', String),
                        Column('listed_in', String),
                        Column('director', String),
                        Column('country', String),
                        Column('cast', String))
    
    # Netflix Directors table
    netflix_directors = Table('netflix_directors', meta,
                              Column('show_id', String),
                              Column('director', String))
    
    # Netflix Genre table
    netflix_genre = Table('netflix_genre', meta,
                          Column('show_id', String),
                          Column('genre', String))

    # Netflix Main table
    netflix = Table('netflix', meta,
                    Column('show_id', String),
                    Column('type', String),
                    Column('title', String),
                    Column('date_added', String),
                    Column('release_year', Integer),
                    Column('rating', String),
                    Column('duration', String),
                    Column('description', String))
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS netflix_country (
        show_id VARCHAR,
        country VARCHAR
    );
    """
    with engine.connect() as conn:
        conn.execute(text(create_table_query))
    print("Table 'netflix_country' created successfully!")

    meta.create_all(engine)
    print("Tables created successfully!")

# 2. Function to process and populate the Netflix Directors table
def populate_directors():
    query = """
    INSERT INTO netflix_directors (show_id, director)
    SELECT show_id, director
    FROM netflix_raw
    WHERE director IS NOT NULL
    """
    with engine.connect() as conn:
        conn.execute(text(query))
    print("Directors data inserted successfully!")

# 3. Function to split genres and insert them into netflix_genre
def populate_genres():
    query = """
    INSERT INTO netflix_genre (show_id, genre)
    SELECT show_id, trim(genre) AS genre
    FROM netflix_raw, 
         unnest(string_to_array(listed_in, ',')) AS genre
    """
    with engine.connect() as conn:
        conn.execute(text(query))
    print("Genres data inserted successfully!")

# 4. Function to populate the Netflix main table and handle nulls
def populate_netflix_table():
    query = """
    WITH cte AS (
        SELECT *,
        ROW_NUMBER() OVER (PARTITION BY title, type ORDER BY show_id) AS rn
        FROM netflix_raw
    )
    INSERT INTO netflix (show_id, type, title, date_added, release_year, rating, duration, description)
    SELECT show_id, type, title, cast(date_added AS date) as date_added, release_year,
           rating, CASE WHEN duration IS NULL THEN rating ELSE duration END as duration, description
    FROM cte
    WHERE rn = 1
    """
    with engine.connect() as conn:
        conn.execute(text(query))
    print("Netflix main table populated successfully!")

# 5. Function to populate missing country data and handle analysis
def populate_country_data_and_analyze():
    # Populating missing country data
    country_query = """
    INSERT INTO netflix_country (show_id, country)
    SELECT nr.show_id, m.country
    FROM netflix_raw nr
    INNER JOIN (
        SELECT director, country
        FROM netflix_country nc
        INNER JOIN netflix_directors nd ON nc.show_id = nd.show_id
        GROUP BY director, country
    ) m ON nr.director = m.director
    WHERE nr.country IS NULL
    """
    with engine.connect() as conn:
        conn.execute(text(country_query))
    print("Missing country data populated!")

    # # Additional analysis queries (e.g., finding the country with the highest number of comedy movies)
    # analysis_query = """
    # SELECT nc.country, COUNT(distinct ng.show_id) as no_of_movies
    # FROM netflix_genre ng
    # INNER JOIN netflix_country nc ON ng.show_id = nc.show_id
    # INNER JOIN netflix n ON ng.show_id = nc.show_id
    # WHERE ng.genre = 'Comedies' AND n.type = 'Movie'
    # GROUP BY nc.country
    # ORDER BY no_of_movies DESC
    # LIMIT 1;
    # """
    # with engine.connect() as conn:
    #     result = conn.execute(text(analysis_query))
    #     for row in result:
    #         print(row)

def drop_all_tables():
    """Drop all tables from the PostgreSQL database."""
    tables = ['netflix_raw', 'netflix_genre', 'netflix', 'netflix_directors', 'netflix_country']  # List all your tables here

    with engine.connect() as conn:
        for table in tables:
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE;"))
                print(f"Table '{table}' dropped successfully.")
            except Exception as e:
                print(f"Error dropping table '{table}': {e}")

def count_movies_tvshows_by_director():
    query = """
    SELECT nd.director,
           COUNT(DISTINCT CASE WHEN n.type='Movie' THEN n.show_id END) AS no_of_movies,
           COUNT(DISTINCT CASE WHEN n.type='TV Show' THEN n.show_id END) AS no_of_tvshow
    FROM netflix n
    INNER JOIN netflix_directors nd ON n.show_id = nd.show_id
    GROUP BY nd.director
    HAVING COUNT(DISTINCT n.type) > 1;
    """
    # query = "SELECT * FROM netflix"
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return pd.DataFrame(result.fetchall(), columns=result.keys())

def country_with_highest_comedy_movies():
    query = """
    SELECT nc.country,
           COUNT(DISTINCT ng.show_id) AS no_of_movies
    FROM netflix_genre ng
    INNER JOIN netflix_country nc ON ng.show_id = nc.show_id
    INNER JOIN netflix n ON ng.show_id = n.show_id
    WHERE ng.genre = 'Comedies' AND n.type = 'Movie'
    GROUP BY nc.country
    ORDER BY no_of_movies DESC
    LIMIT 1;
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return pd.DataFrame(result.fetchall(), columns=result.keys())

def max_movies_by_director_per_year():
    query = """
    WITH cte AS (
        SELECT nd.director,
               EXTRACT(YEAR FROM TO_DATE(date_added, 'Month DD, YYYY')) AS date_year,
               COUNT(n.show_id) AS no_of_movies
        FROM netflix n
        INNER JOIN netflix_directors nd ON n.show_id = nd.show_id
        WHERE type = 'Movie'
        GROUP BY nd.director, EXTRACT(YEAR FROM TO_DATE(date_added, 'Month DD, YYYY'))
    ),
    cte2 AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY date_year ORDER BY no_of_movies DESC, director) AS rn
        FROM cte
    )
    SELECT * FROM cte2 WHERE rn = 1;
    """

    with engine.connect() as conn:
        result = conn.execute(text(query))
        return result.fetchall()

def average_duration_per_genre():
    query = """
    SELECT ng.genre,
           AVG(CAST(REPLACE(duration, ' min', '') AS INT)) AS avg_duration
    FROM netflix n
    INNER JOIN netflix_genre ng ON n.show_id = ng.show_id
    WHERE type = 'Movie'
    GROUP BY ng.genre;
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return pd.DataFrame(result.fetchall(), columns=result.keys())

def directors_horror_comedy_movies():
    query = """
    SELECT nd.director,
           COUNT(DISTINCT CASE WHEN ng.genre = 'Comedies' THEN n.show_id END) AS no_of_comedy,
           COUNT(DISTINCT CASE WHEN ng.genre = 'Horror Movies' THEN n.show_id END) AS no_of_horror
    FROM netflix n
    INNER JOIN netflix_genre ng ON n.show_id = ng.show_id
    INNER JOIN netflix_directors nd ON n.show_id = nd.show_id
    WHERE type = 'Movie' AND ng.genre IN ('Comedies', 'Horror Movies')
    GROUP BY nd.director
    HAVING COUNT(DISTINCT ng.genre) = 2;
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return pd.DataFrame(result.fetchall(), columns=result.keys())


# # To execute the functions in order
# if __name__ == "__main__":
#     # drop_all_tables()
#     create_tables()
#     load_data_to_postgres('netflix_titles.csv')  # Load data from CSV file

#     # Populate related tables
#     populate_directors()
#     populate_genres()
#     populate_netflix_table()
#     populate_country_data_and_analyze()

def count_rows_in_netflix_raw():
    query = "SELECT * FROM netflix_country;"
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        # count = result.scalar()
        # return count
        return pd.DataFrame(result.fetchall(), columns=result.keys())


# def load_netflix_data():
#     query = """
#     INSERT INTO netflix (show_id, type, title, date_added, release_year, rating, duration, description)
#     SELECT show_id, type, title, date_added, release_year, rating, duration, description
#     FROM netflix_raw;
#     """
    
#     with engine.connect() as conn:
#         conn.execute(text(query))
#         print("Data loaded into 'netflix' table.")
# load_netflix_data()

# def load_netflix_country_data():
#     query = """
#     INSERT INTO netflix_country (show_id, country)
#     SELECT show_id, country
#     FROM netflix_raw
#     WHERE country IS NOT NULL;
#     """
    
#     with engine.connect() as conn:
#         conn.execute(text(query))
#         print("Data loaded into 'netflix_country' table.")

# load_netflix_country_data()


# print("Number of rows in netflix_raw:", count_rows_in_netflix_raw())

print("Count of Movies and TV Shows by Director:")
print(count_movies_tvshows_by_director())

print("\nCountry with Highest Number of Comedy Movies:")
print(country_with_highest_comedy_movies())

print("\nDirector with Maximum Movies Released per Year:")
print(max_movies_by_director_per_year())

print("\nAverage Duration of Movies in Each Genre:")
print(average_duration_per_genre())
    
print("\nDirectors Who Created Both Horror and Comedy Movies:")
print(directors_horror_comedy_movies())