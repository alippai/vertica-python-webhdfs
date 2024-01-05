from multiprocessing import Process

import pytest
import requests
import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
import vertica_python as vp
import json

app = FastAPI()


@app.get("/{full_path:path}")
def all_get(request: Request, full_path: str):
    print(f"full path {full_path}")
    print(f"query param {request.query_param}")
    JSONResponse(
        status_code=404,
        content={
            "RemoteException": {
                "exception": "FileNotFoundException",
                "javaClassName": "java.io.FileNotFoundException",
                "message": "File does not exist: /foo/a.patch",
            }
        },
    )


@app.put("/{full_path:path}")
async def all_put(request: Request, full_path: str):
    print(f"full path {full_path}")
    print(f"query param {request.query_param}")
    if request.query_param["op"] == "MKDIRS":
        return {"boolean": True}
    if request.query_param["op"] == "CREATE":
        await request.body()
        return Response(status_code=201)


@app.delete("/{full_path:path}")
def all_delete(request: Request, full_path: str):
    print(f"full path {full_path}")
    print(f"query param {request.query_param}")
    return {"boolean": True}


def run_server():
    uvicorn.run(app)
    print('server running')


@pytest.fixture
def server():
    proc = Process(target=run_server, args=(), daemon=True)
    proc.start()
    yield
    proc.kill()  # Cleanup after test


def test_read_main(server):
    connection = vp.connect(
        **{"host": "127.0.0.1", "port": 5433, "user": "dbadmin", "database": "VMart"}
    )
    print(connection.cursor().execute("select 1").fetchall())
    print('trying to connect server')
    print(
        connection.cursor()
        .execute(
            "EXPORT TO PARQUET(directory = 'webhdfs://127.0.0.1:8000/virtualdata') AS SELECT 1 AS account_id"
        )
        .fetchall()
    )
