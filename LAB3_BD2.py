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

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
def main():
    driver = GraphDatabase.driver(URI, auth=AUTH)

    with driver.session(database="neo4j") as session:



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




    driver.close()

if __name__ == "__main__":
    main()
