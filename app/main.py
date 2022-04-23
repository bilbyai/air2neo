from fastapi import FastAPI

from .airtable_ingest import start_ingest

app = FastAPI()


@app.get("/", status_code=200)
def hello():
    return 'healthy'


@app.get("/airtable-ingest")
def ingest():
    start_ingest()
    return 'OK'


@app.get("/airtable-nuke-and-ingest")
def nuke_and_ingest():
    ingest()
    return 'OK'
