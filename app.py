from flask import Flask, jsonify
import pandas as pd
from sqlalchemy import create_engine, text
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Database connection URL
db_url = 'postgresql://netflix_data_cleaning_gvro_user:Vbxny2At2GGehYt1v2ADitV8WUAcs55q@dpg-crpb8cij1k6c73c33890-a.oregon-postgres.render.com/netflix_data_cleaning_gvro'
engine = create_engine(db_url)

# Function to execute SQL and return DataFrame
def execute_query(query):
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return pd.DataFrame(result.fetchall(), columns=result.keys())

# API routes for each of the 5 functions
@app.route('/count_movies_tvshows_by_director', methods=['GET'])
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
    result = execute_query(query)
    return result.to_json(orient="records")

@app.route('/country_with_highest_comedy_movies', methods=['GET'])
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
    result = execute_query(query)
    return result.to_json(orient="records")

@app.route('/max_movies_by_director_per_year', methods=['GET'])
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
        SELECT * ,
               ROW_NUMBER() OVER (PARTITION BY date_year ORDER BY no_of_movies DESC, director) AS rn
        FROM cte
    )
    SELECT * FROM cte2 WHERE rn = 1;
    """
    result = execute_query(query)
    return result.to_json(orient="records")

@app.route('/average_duration_per_genre', methods=['GET'])
def average_duration_per_genre():
    query = """
    SELECT ng.genre,
           AVG(CAST(REPLACE(duration, ' min', '') AS INT)) AS avg_duration
    FROM netflix n
    INNER JOIN netflix_genre ng ON n.show_id = ng.show_id
    WHERE type = 'Movie'
    GROUP BY ng.genre;
    """
    result = execute_query(query)
    return result.to_json(orient="records")

@app.route('/directors_horror_comedy_movies', methods=['GET'])
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
    result = execute_query(query)
    return result.to_json(orient="records")

if __name__ == '__main__':
    app.run(debug=True)
