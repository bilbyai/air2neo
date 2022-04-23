FROM python:3.9.10-slim

ENV APP_HOME /app

COPY . ./app

RUN apt-get update \
  && apt-get install gcc -y \
  && apt-get clean

ENV PYTHONPATH /app

WORKDIR $APP_HOME

RUN pip3 install pipenv
RUN pipenv install --deploy --system

EXPOSE $PORT
CMD exec gunicorn --bind :$PORT --workers 1 --worker-class uvicorn.workers.UvicornWorker --threads 8 app.main:app