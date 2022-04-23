from fastapi import FastAPI, Response

from .airtable_ingest import start_ingest

app = FastAPI()

@app.get("/")
def hello():
    return Response("healthy",
                    status=200,
                    mimetype="text/plain")

@app.get("/airtable-ingest")
def ingest():
    start_ingest()

@app.get("/airtable-nuke-and-ingest")
def nuke_and_ingest():
    ingest()
