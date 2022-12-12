FROM python:3.9.12

ENV PYTHONUNBUFFERED 1

WORKDIR /app
ADD . /app

RUN pip install -r requirements.txt

CMD [ "uwsgi","app.ini" ]
