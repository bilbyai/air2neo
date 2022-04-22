from fastapi import FastAPI, Response

from . import airtable_ingest

app = FastAPI()

@app.get("/")
def hello():
    return Response("healthy",
                    status=200,
                    mimetype="text/plain")

@app.get("/airtable-ingest")
def ingest():
    pass

@app.get("/airtable-clean-ingest")
def clean_ingest():
    pass
