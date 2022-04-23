from fastapi import BackgroundTasks, FastAPI

from .airtable_ingest import run_airtable_to_neo4j_ingest_job

app = FastAPI()


@app.get("/", status_code=200)
def hello() -> str:
    ''' default health check endpoint.

    Returns:
        str: 'healthy'.
    '''
    return {'message': 'OK'}


@app.get("/airtable-ingest")
async def ingest(background_tasks: BackgroundTasks) -> str:
    ''' This function is used to ingest data from Airtable into Neo4j.
    Runs in async.

    Returns:
        str: If everything was OK, it returns 'OK'.
    '''
    background_tasks.add_task(run_airtable_to_neo4j_ingest_job)
    return {'message': 'OK'}


@app.get("/airtable-nuke-and-ingest")
async def nuke_and_ingest(background_tasks: BackgroundTasks) -> str:
    ''' This function is used to nuke the Neo4j database and ingest data from
    Airtable into Neo4j.
    Nuke means drop everything.
    Runs in async.

    Returns:
        str: If everything was OK, it returns 'OK'.
    '''
    background_tasks.add_task(run_airtable_to_neo4j_ingest_job, nuke=True)
    return {'message': 'OK'}
