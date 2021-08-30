FROM python:3.7

WORKDIR .

COPY requirements.txt requirements.txt
RUN python -m pip install --upgrade pip
RUN pip install -r requirements.txt
RUN python -m grpc_tools.protoc -I./ --python_out=./ --grpc_python_out=./ location.proto


EXPOSE 5005

COPY . .
CMD ["python", "main.py"]