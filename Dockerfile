FROM python:3.9.12

ENV PYTHONUNBUFFERED 1

WORKDIR /app
ADD . /app

RUN apt-get update
RUN apt-get install -y --no-install-recommends \
        libatlas-base-dev gfortran nginx supervisor

RUN pip install -r requirements.txt

RUN useradd --no-create-home nginx

RUN rm /etc/nginx/sites-enabled/default
RUN rm -r /root/.cache

COPY ./files/nginx.conf /etc/nginx/
COPY ./files/uwsgi.ini /etc/uwsgi/
COPY ./files/flask.conf /etc/nginx/conf.d/
COPY ./files/supervisord.conf /etc/

WORKDIR /app

CMD ["/usr/bin/supervisord"]
