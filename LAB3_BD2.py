from neo4j import GraphDatabase

# URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
URI = "neo4j+s://5e7134fe.databases.neo4j.io"
AUTH = ("neo4j", "vg4OqlW3WQ5fs5_lDt2xKgHTz_09qix4HLGqzOrwEpg")

employee_threshold=10


def main():
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session(database="neo4j") as session:
            for i in range(100):
                name = f"Thor{i}"
                org_id = session.execute_write(employ_person_tx, name)
                print(f"User {name} added to organization {org_id}")
            

            # movie = session.execute_read(find_movie_tx, "The Matrix")
            # print(movie)


def employ_person_tx(tx, name):
    # Create new Person node with given name, if not exists already
    result = tx.run("""
        MERGE (p:Person {name: $name})
        RETURN p.name AS name
        """, name=name
    )

    # Obtain most recent organization ID and the number of people linked to it
    result = tx.run("""
        MATCH (o:Organization)
        RETURN o.id AS id, COUNT{(p:Person)-[r:WORKS_FOR]->(o)} AS employees_n
        ORDER BY o.created_date DESC
        LIMIT 1
    """)
    org = result.single()

    if org is not None and org["employees_n"] == 0:
        raise Exception("Most recent organization is empty.")
        # Transaction will roll back -> not even Person is created!

    # If org does not have too many employees, add this Person to that
    if org is not None and org.get("employees_n") < employee_threshold:
        result = tx.run("""
            MATCH (o:Organization {id: $org_id})
            MATCH (p:Person {name: $name})
            MERGE (p)-[r:WORKS_FOR]->(o)
            RETURN $org_id AS id
            """, org_id=org["id"], name=name
        )

    # Otherwise, create a new Organization and link Person to it
    else:
        result = tx.run("""
            MATCH (p:Person {name: $name})
            CREATE (o:Organization {id: randomuuid(), created_date: datetime()})
            MERGE (p)-[r:WORKS_FOR]->(o)
            RETURN o.id AS id
            """, name=name
        )

    # Return the Organization ID to which the new Person ends up in
    return result.single()["id"]

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