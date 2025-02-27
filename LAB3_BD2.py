# Grupo: Bededos
# Gerardo Pineda 22880
# Marlon Hernández 15177
# Angel Martin Ortega Yung 18020

import random
from datetime import date, timedelta
from neo4j import GraphDatabase

# ------------------------------------------------------------------------------
# Configuración
# ------------------------------------------------------------------------------
URI = "neo4j+s://5e7134fe.databases.neo4j.io"
AUTH = ("neo4j", "vg4OqlW3WQ5fs5_lDt2xKgHTz_09qix4HLGqzOrwEpg")

# ------------------------------------------------------------------------------
# Funciones para Personas
# ------------------------------------------------------------------------------
NUM_PEOPLE = 20
NUM_MOVIES = 5

def random_date(start_year=1950, end_year=2000):
    start_date = date(start_year, 1, 1)
    end_date   = date(end_year, 12, 31)
    delta_days = (end_date - start_date).days
    random_days = random.randrange(delta_days)
    return start_date + timedelta(days=random_days)

def random_label_set():
    choice = random.choice(["Actor", "Director", "Both"])
    if choice == "Both":
        return ["Actor", "Director"]
    else:
        return [choice]

def random_runtime():
    return random.randint(80, 180)

def random_rating():
    return round(random.uniform(1.0, 10.0), 1)


def create_movie_tx(tx, movie_data):
    query = """
    CREATE (m:Movie {
        title: $title,
        tmdbId: $tmdbId,
        released: datetime($released),
        imdbRating: $imdbRating,
        movieId: $movieId,
        year: $year,
        imdbId: $imdbId,
        runtime: $runtime,
        countries: $countries,
        imdbVotes: $imdbVotes,
        url: $url,
        revenue: $revenue,
        plot: $plot,
        poster: $poster,
        budget: $budget,
        languages: $languages
    })
    RETURN m
    """
    tx.run(query, **movie_data)


def find_movie_tx(tx, title):
    query = """
    MATCH (m:Movie {title: $title})
    RETURN m
    """
    result = tx.run(query, title=title)
    return result.single()[0]

def find_user_tx(tx, name):
    query = """
    MATCH (u:User {name: $name})
    RETURN u
    """
    result = tx.run(query, name=name)
    return result.single()[0]

def find_user_tx(tx, name):
    query = """
    MATCH (u:User {name: $name})
    RETURN u
    """
    result = tx.run(query, name=name)
    return result.single()[0]

def find_user_movie_rating(tx, name):
    query = """
    MATCH (u:User {name: $name})-[r:RATED]->(m:Movie)
    RETURN u, r, m
    """
    result = tx.run(query, name=name)
    return result.data()

def DELETE_DATABASE(tx):
    query = """
    MATCH (n)
    DETACH DELETE n
    """
    tx.run(query)
# ------------------------------------------------------------------------------
# Función de crear nodo Person
# ------------------------------------------------------------------------------
def create_person_tx(tx, name, tmdbId, born, died, bornIn, url, imdbId, bio, poster, labels):
    labels_str = ":Person" + "".join(f":{label}" for label in labels)

    query = f"""
    MERGE (p{labels_str} {{ tmdbId: $tmdbId }})
    ON CREATE SET
        p.name   = $name,
        p.born   = date($born),
        p.died   = CASE WHEN $died IS NULL THEN NULL ELSE date($died) END,
        p.bornIn = $bornIn,
        p.url    = $url,
        p.imdbId = $imdbId,
        p.bio    = $bio,
        p.poster = $poster
    RETURN p
    """

    result = tx.run(
        query,
        name=name,
        tmdbId=tmdbId,
        born=born.isoformat() if born else None,
        died=died.isoformat() if died else None,
        bornIn=bornIn,
        url=url,
        imdbId=imdbId,
        bio=bio,
        poster=poster
    )
    return result.single()[0]


def create_user(tx, name):
    query = """
    CREATE (u:User {name: $name, userId: randomUUID()})
    RETURN u
    """
    result = tx.run(query, name=name)
    return result.single()[0]

def create_genre(tx, genre):
    query = """
    CREATE (g:Genre {name: $genre})
    RETURN g
    """
    result = tx.run(query, genre=genre)
    return result.single()[0]

def get_movie_ids(tx):
    query = "MATCH (m:Movie) RETURN m.movieId"
    result = tx.run(query)
    return [record["m.movieId"] for record in result]


def create_user_ratings(tx, name, movie_ids):
    for movie_id in movie_ids:
        if random.random() < 0.5:
            rating = random.randint(1, 5)
            query = """
            MATCH (u:User {name: $name}), (m:Movie {movieId: $movie_id})
            MERGE (u)-[r:RATED {rating: $rating}]->(m)
            RETURN u, m, r
            """
            tx.run(query, name=name, movie_id=movie_id, rating=rating)


def create_movies_with_genres(tx, movie_ids, GENRES):
    for movie_id in movie_ids:
        num_genres = random.randint(1, 3)
        genres = random.sample(GENRES, num_genres)

        query = """
        MATCH (m:Movie {movieId: $movie_id})
        WITH m
        UNWIND $genres AS genre
        MATCH (g:Genre {name: genre})
        MERGE (m)-[:IN_GENRE]->(g)
        RETURN m, g
        """
        tx.run(query, movie_id=movie_id, genres=genres) 



def create_relationships(tx):
    query = """
    MATCH (p:Person)
    WHERE "Actor" IN labels(p) OR "Director" IN labels(p)
    RETURN p.name AS name, labels(p) AS labels, p.tmdbId AS tmdbId
    """
    people = tx.run(query).data()

    for person in people:
        name = person["name"]
        tmdbId = person["tmdbId"]
        labels = person["labels"]

        movie_query = "MATCH (m:Movie) RETURN m.movieId AS movieId ORDER BY rand() LIMIT 1"
        movie_result = tx.run(movie_query).single()

        if movie_result:
            movie_id = movie_result["movieId"]

            if "Actor" in labels:
                tx.run("""
                MATCH (p:Person {tmdbId: $tmdbId}), (m:Movie {movieId: $movieId})
                MERGE (p)-[:ACTED_IN]->(m)
                """, tmdbId=tmdbId, movieId=movie_id)

            if "Director" in labels:
                tx.run("""
                MATCH (p:Person {tmdbId: $tmdbId}), (m:Movie {movieId: $movieId})
                MERGE (p)-[:DIRECTED]->(m)
                """, tmdbId=tmdbId, movieId=movie_id)




# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
def main():
    driver = GraphDatabase.driver(URI, auth=AUTH)

    with driver.session(database="neo4j") as session:

        session.execute_write(DELETE_DATABASE)

 # ------------------------------------------------------------------------------
# Loop para crear nodos Movie
# ------------------------------------------------------------------------------
        for i in range(NUM_MOVIES):
            movie_data = {
                "title": f"Movie_{i}",
                "tmdbId": 2000 + i,
                "released": random_date(1980, 2023).isoformat(),
                "imdbRating": random_rating(),
                "movieId": 6000 + i,
                "year": random.randint(1980, 2023),
                "imdbId": 70000 + i,
                "runtime": random_runtime(),
                "countries": ["USA", "UK"],
                "imdbVotes": random.randint(1000, 100000),
                "url": f"https://www.example.com/movie_{i}",
                "revenue": round(random.uniform(1.0, 1000.0), 2),
                "plot": f"Plot for Movie_{i}",
                "poster": f"https://www.example.com/movie_poster_{i}.jpg",
                "budget": round(random.uniform(1.0, 500.0), 2),
                "languages": ["English", "French"]
            }
            session.execute_write(create_movie_tx, movie_data)
            print(
                f"Nodo movie creado con name: {movie_data['title']}, "
                f"tmdbId: {movie_data['tmdbId']}"
            )


# ------------------------------------------------------------------------------
# Loop para crear nodos Person
# ------------------------------------------------------------------------------
        for i in range(NUM_PEOPLE):
            name    = f"Person_{i}"
            tmdbId  = 1000 + i
            born    = random_date()
            died    = random_date(born.year + 1, 2020) if random.random() < 0.3 else None
            bornIn  = "Some City"
            url     = f"https://www.example.com/person_{i}"
            imdbId  = 50000 + i
            bio     = f"Bio for {name}"
            poster  = f"https://www.example.com/poster_{i}.jpg"
            labels  = random_label_set()

            # Escribir en base de datos
            node = session.execute_write(
                create_person_tx,
                name,
                tmdbId,
                born,
                died,
                bornIn,
                url,
                imdbId,
                bio,
                poster,
                labels
            )
            print(
                f"Nodo creado con labels: {labels}, "
                f"name: {node['name']}, "
                f"tmdbId: {node['tmdbId']}"
            )

# ------------------------------------------------------------------------------
        moviesID = session.execute_read(get_movie_ids)


        # ------ CREATE USERS ------
        # ------ CREATE AND CREATE USER RATING RELATION -----
        for i in range(5):
            user = "User_" + str(i)
            node = session.execute_write(create_user, user)
            session.execute_write(create_user_ratings, user, moviesID)


        # ------ CREATE GENRES ------ 

        GENRES = [
            "Action", "Adventure", "Sci-Fi", "Drama", "Thriller",
            "Crime", "Fantasy", "Mystery", "Romance", "Animation"
        ]

        for g in GENRES:
            session.execute_write(create_genre, g)

        # ------ CREATE RELATION MOVIES WITH GENRES ------
        session.execute_write(create_movies_with_genres, moviesID, GENRES)

        # ------ CREATE RELATIONSHIPS PERSON -> MOVIES ------
        session.execute_write(create_relationships)

        print("# ------------------------------------------------------------------------------")
        print("BUSQUEDA PELICULA")
        result = session.execute_read(find_movie_tx, "Movie_1")
        print(result)
        print("# ------------------------------------------------------------------------------")
        print("BUSQUEDA USUARIO")
        result = session.execute_read(find_user_tx, "User_1")
        print(result)
        print("# ------------------------------------------------------------------------------")
        print("BUSQUEDA USUARIO RELACION MOVIE")
        result = session.execute_write(find_user_movie_rating, "User_1")
        print(result)




    driver.close()

if __name__ == "__main__":
    main()
