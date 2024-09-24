# import sqlalchemy as sal
# import pandas as pd
# df = pd.read_csv('netflix_titles.csv')
# engine = sal.create_engine(
#     'mssql://ANKIT\SQLEXPRESS/master?driver=ODBC+DRIVER+17+FOR+SQL+SERVER')
# conn = engine.connect()

# df.to_sql('netflix_raw', con=conn, index=False, if_exists='append')
# conn.close()

# df.head()
# df[df.show_id == 's5023']
# max(df.description.dropna().str.len())
# df.isna().sum()

# import sqlalchemy as sal
# import pandas as pd
# from sqlalchemy import create_engine, Column, String, Integer, Table, MetaData

# db_url = 'postgresql://netflix_data_cleaning_user:BxOcAVthJBoGJ7vSrdsY3KXHKW0vmEHZ@dpg-crpa2mu8ii6s73cegaeg-a.oregon-postgres.render.com/netflix_data_cleaning'

# engine = create_engine(db_url)

# def create_netflix_table():
#     """Create the netflix_raw table in the PostgreSQL database."""
#     metadata = MetaData()
    
#     netflix_raw = Table(
#         'netflix_raw', metadata,
#         Column('show_id', String(10), primary_key=True),
#         Column('type', String(10)),
#         Column('title', String(200)),
#         Column('director', String(250)),
#         Column('cast', String(1000)),
#         Column('country', String(150)),
#         Column('date_added', String(20)),
#         Column('release_year', Integer),
#         Column('rating', String(10)),
#         Column('duration', String(10)),
#         Column('listed_in', String(100)),
#         Column('description', String(500))
#     )

#     metadata.create_all(engine)
#     print("Table 'netflix_raw' created successfully.")

# def load_data_to_postgres(csv_file):
#     """Load data from a CSV file into the netflix_raw table in PostgreSQL."""
#     df = pd.read_csv(csv_file)
    
#     with engine.connect() as conn:
#         df.to_sql('netflix_raw', con=conn, index=False, if_exists='append')

#     print(df.head())
#     print(df[df.show_id == 's5023'])
#     print(max(df.description.dropna().str.len()))
#     print(df.isna().sum())


# def query_show_by_id(show_id):
#     query = f"SELECT * FROM netflix_raw"
#     with engine.connect() as conn:
#         result = conn.execute(sal.text(query), {'show_id': show_id})
#         # Fetch all results
#         rows = result.fetchall()
        
#         # Convert to a DataFrame for easier viewing
#         df_result = pd.DataFrame(rows, columns=result.keys())
#         return df_result

# if __name__ == "__main__":
#     # create_netflix_table()  
#     # load_data_to_postgres('netflix_titles.csv')  # Load data from CSV

#     show_id_to_query = 's5023'
#     result_df = query_show_by_id(show_id_to_query)
    
#     print(result_df)


import sqlalchemy as sal
import pandas as pd
from sqlalchemy import create_engine, Column, String, Integer, Table, MetaData, text

# Database connection URL
db_url = 'postgresql://netflix_data_cleaning_6jwk_user:rJvOnvo8jFk08C84x71MbQuQIOenUE2q@dpg-crpav2m8ii6s73ceu660-a.oregon-postgres.render.com/netflix_data_cleaning_6jwk'

# Create a SQLAlchemy engine
engine = create_engine(db_url)

def create_netflix_table():
    """Create the netflix_raw table in the PostgreSQL database."""
    metadata = MetaData()
    
    netflix_raw = Table(
        'netflix_raw', metadata,
        Column('show_id', String(10), primary_key=True),
        Column('type', String(10)),
        Column('title', String(200)),
        Column('director', String(250)),
        Column('cast', String(1000)),
        Column('country', String(150)),
        Column('date_added', String(20)),
        Column('release_year', Integer),
        Column('rating', String(10)),
        Column('duration', String(10)),
        Column('listed_in', String(100)),
        Column('description', String(500))
    )

    metadata.create_all(engine)
    print("Table 'netflix_raw' created successfully.")

def create_directors_table():
    """Create the netflix_directors table in the PostgreSQL database."""
    metadata = MetaData()
    
    netflix_directors = Table(
        'netflix_directors', metadata,
        Column('show_id', String(10)),
        Column('director', String(250))
    )

    # Create the table in the database
    metadata.create_all(engine)
    print("Table 'netflix_directors' created successfully.")

def create_genre_table():
    """Create the netflix_raw table in the PostgreSQL database."""
    metadata = MetaData()
    
    netflix_raw = Table(
        'netflix_genre', metadata,
        Column('show_id', String(10)),
        Column('genre', String(200)),
    )

    metadata.create_all(engine)
    print("Table 'create_genre_table' created successfully.")

def create_country_table():
    """Create the netflix_raw table in the PostgreSQL database."""
    metadata = MetaData()
    
    netflix_raw = Table(
        'netflix_country', metadata,
        Column('show_id', String(10)),
        Column('country', String(200)),
    )

    metadata.create_all(engine)
    print("Table 'create_country_table' created successfully.")

def create_netflix_table():
    """Create the netflix_raw table in the PostgreSQL database."""
    metadata = MetaData()
    
    netflix_raw = Table(
        'netflix', metadata,
        Column('show_id', String(10)),
        Column('country', String(200)),
    )

    metadata.create_all(engine)
    print("Table 'create_country_table' created successfully.")

def load_data_to_postgres(csv_file):
    """Load data from a CSV file into the netflix_raw table in PostgreSQL."""
    # Read the CSV file
    df = pd.read_csv(csv_file)
    
    with engine.connect() as conn:
        df.to_sql('netflix_raw', con=conn, index=False, if_exists='append')

    print("Data loaded into 'netflix_raw' table.")

def run_data_cleaning_queries():
    """Run a sequence of SQL commands for data cleaning and transformation."""

    queries = [
        # Check for duplicate show_id
        "SELECT show_id, COUNT(*) FROM netflix_raw GROUP BY show_id HAVING COUNT(*) > 1",
        
        # Check for duplicate title + type combinations
        """SELECT * FROM netflix_raw
           WHERE CONCAT(UPPER(title), type) IN (
               SELECT CONCAT(UPPER(title), type) 
               FROM netflix_raw
               GROUP BY UPPER(title), type
               HAVING COUNT(*) > 1
           )
           ORDER BY title""",
        
        # Create a cleaned table with no duplicate entries
        """WITH cte AS (
               SELECT *, ROW_NUMBER() OVER (PARTITION BY title, type ORDER BY show_id) AS rn
               FROM netflix_raw
           )
           SELECT show_id, type, title, CAST(date_added AS DATE) AS date_added, release_year,
                  rating, CASE WHEN duration IS NULL THEN rating ELSE duration END AS duration, description
           INTO netflix
           FROM cte""",
        
        # Split the 'listed_in' column into a separate table for genres
        """INSERT INTO netflix_genre (show_id, genre)
           SELECT show_id, TRIM(genre)
           FROM netflix_raw, unnest(string_to_array(listed_in, ',')) AS genre""",
        
        # Insert missing country values based on director info
        """INSERT INTO netflix_country (show_id, country)
           SELECT nr.show_id, m.country 
           FROM netflix_raw nr
           INNER JOIN (
               SELECT director, country
               FROM netflix_country nc
               INNER JOIN netflix_directors nd ON nc.show_id = nd.show_id
               GROUP BY director, country
           ) m ON nr.director = m.director
           WHERE nr.country IS NULL""",
        
        # Select rows where duration is null
        "SELECT * FROM netflix_raw WHERE duration IS NULL"
    ]
    
    # Execute each query in sequence
    with engine.connect() as conn:
        for query in queries:
            conn.execute(text(query))
            print(f"Query executed successfully: {query[:60]}...")

def count_movies_and_tvshows_by_director():
    query = """
    SELECT nd.director,
           COUNT(DISTINCT CASE WHEN n.type='Movie' THEN n.show_id END) AS no_of_movies,
           COUNT(DISTINCT CASE WHEN n.type='TV Show' THEN n.show_id END) AS no_of_tvshow
    FROM netflix n
    INNER JOIN netflix_directors nd ON n.show_id = nd.show_id
    GROUP BY nd.director
    HAVING COUNT(DISTINCT n.type) > 0
    """
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return pd.DataFrame(result.fetchall(), columns=result.keys())
    
def country_with_highest_comedy_movies():
    query = """
    SELECT nc.country, COUNT(DISTINCT ng.show_id) AS no_of_movies
    FROM netflix_genre ng
    INNER JOIN netflix_country nc ON ng.show_id = nc.show_id
    INNER JOIN netflix n ON ng.show_id = n.show_id
    WHERE ng.genre = 'Comedies' AND n.type = 'Movie'
    GROUP BY nc.country
    ORDER BY no_of_movies DESC
    LIMIT 1
    """
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return pd.DataFrame(result.fetchall(), columns=result.keys())

def max_movies_by_director_per_year():
    query = """
    WITH cte AS (
        SELECT nd.director, EXTRACT(YEAR FROM date_added) AS date_year, COUNT(n.show_id) AS no_of_movies
        FROM netflix n
        INNER JOIN netflix_directors nd ON n.show_id = nd.show_id
        WHERE n.type = 'Movie'
        GROUP BY nd.director, EXTRACT(YEAR FROM date_added)
    ),
    cte2 AS (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY date_year ORDER BY no_of_movies DESC, director) AS rn
        FROM cte
    )
    SELECT * FROM cte2 WHERE rn = 1
    """
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return pd.DataFrame(result.fetchall(), columns=result.keys())

def average_duration_by_genre():
    query = """
    SELECT ng.genre, AVG(CAST(REPLACE(duration, ' min', '') AS INT)) AS avg_duration
    FROM netflix n
    INNER JOIN netflix_genre ng ON n.show_id = ng.show_id
    WHERE n.type = 'Movie'
    GROUP BY ng.genre
    """
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return pd.DataFrame(result.fetchall(), columns=result.keys())

def directors_with_horror_and_comedy():
    query = """
    SELECT nd.director,
           COUNT(DISTINCT CASE WHEN ng.genre='Comedies' THEN n.show_id END) AS no_of_comedy,
           COUNT(DISTINCT CASE WHEN ng.genre='Horror Movies' THEN n.show_id END) AS no_of_horror
    FROM netflix n
    INNER JOIN netflix_genre ng ON n.show_id = ng.show_id
    INNER JOIN netflix_directors nd ON n.show_id = nd.show_id
    WHERE n.type = 'Movie' AND ng.genre IN ('Comedies', 'Horror Movies')
    GROUP BY nd.director
    HAVING COUNT(DISTINCT ng.genre) = 2
    """
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return pd.DataFrame(result.fetchall(), columns=result.keys())


def execute(query):
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return pd.DataFrame(result.fetchall(), columns=result.keys())

if __name__ == "__main__":
    q = '''
    SELECT * FROM NETFLIX
    '''
    execute(q)
    # create_netflix_table() 
    # load_data_to_postgres('netflix_titles.csv') 
    # create_genre_table()
    # create_country_table()
    # create_directors_table()
    # run_data_cleaning_queries()

    # # Clean data
    # clean_data()

    # # Execute and print results of the defined queries
    # print("Directors count of movies and TV shows:")
    # print(count_movies_and_tvshows_by_director())

    # print("\nCountry with the highest number of comedy movies:")
    # print(country_with_highest_comedy_movies())

    # print("\nDirectors with maximum movies released per year:")
    # print(max_movies_by_director_per_year())

    # print("\nAverage duration of movies in each genre:")
    # print(average_duration_by_genre())

    # print("\nDirectors with horror and comedy movies:")
    # print(directors_with_horror_and_comedy())
