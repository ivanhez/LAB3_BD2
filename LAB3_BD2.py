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

# ------------------------------------------------------------------------------
# Función de crear nodo Person
# ------------------------------------------------------------------------------
def create_person_tx(tx, name, tmdbId, born, died, bornIn, url, imdbId, bio, poster, labels):
    labels_str = ":Person" + "".join(f":{label}" for label in labels)

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
    return result.single()

if __name__ == "__main__":
    main()
