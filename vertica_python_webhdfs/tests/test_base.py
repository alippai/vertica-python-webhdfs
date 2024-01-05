from multiprocessing import Process, Request

import pytest
import requests
import uvicorn
from fastapi import FastAPI
import vertica_python as vp

app = FastAPI

@app.get('{full_path:path}')
def all_get(request: Request, full_path: str):
    print(f"full path {full_path}")
    print(f"query param {request.query_param}")

@app.put('{full_path:path}')
def all_put(request: Request, full_path: str):
    print(f"full path {full_path}")
    print(f"query param {request.query_param}")

@app.delete('{full_path:path}')
def all_delete(request: Request, full_path: str):
    print(f"full path {full_path}")
    print(f"query param {request.query_param}")

def run_server():
    uvicorn.run(app)


@pytest.fixture
def server():
    proc = Process(target=run_server, args=(), daemon=True)
    proc.start() 
    yield
    proc.kill() # Cleanup after test


def test_read_main(server):
    response = requests.get("http://localhost:8000/")
    connection = vp.connect(**{'host': '127.0.0.1',
             'port': 5433,
             'user': 'dbadmin',
             'database': 'VMart'
             })
    print(connection.cursor().execute('select 1').fetchall())
    assert response.status_code == 200
    assert response.json() == {"msg": "Hello World"}
    