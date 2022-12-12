FROM python:3.9.12

ENV PYTHONUNBUFFERED 1

WORKDIR /app
ADD . /app

RUN pip install -r requirements.txt

EXPOSE 5000

CMD [ "python","main.py","&" ]
