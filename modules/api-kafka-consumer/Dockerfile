FROM python:3.7

RUN mkdir /app

WORKDIR /app

COPY requirements.txt requirements.txt

RUN python -m pip install --upgrade pip

RUN pip install -r requirements.txt

COPY . /app

CMD ["python", "main.py"]