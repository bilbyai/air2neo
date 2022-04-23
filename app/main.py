from fastapi import FastAPI

from .airtable_ingest import run_airtable_to_neo4j_ingest_job

app = FastAPI()


@app.get("/", status_code=200)
def hello() -> str:
    ''' default health check endpoint.

    Returns:
        str: 'healthy'.
    '''
    return 'healthy'


@app.get("/airtable-ingest")
def ingest() -> str:
    ''' This function is used to ingest data from Airtable into Neo4j.

    Returns:
        str: If everything was OK, it returns 'OK'.
    '''
    run_airtable_to_neo4j_ingest_job()
    return 'OK'


@app.get("/airtable-nuke-and-ingest")
def nuke_and_ingest():
    ''' This function is used to nuke the Neo4j database and ingest data from
    Airtable into Neo4j.
    Nuke means drop everything.

    Returns:
        str: If everything was OK, it returns 'OK'.
    '''
    run_airtable_to_neo4j_ingest_job(nuke=True)
    return 'OK'
