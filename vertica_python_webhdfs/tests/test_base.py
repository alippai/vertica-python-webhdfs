from multiprocessing import Process

import pytest
import requests
import uvicorn
from fastapi import FastAPI, Request, Response
import vertica_python as vp
from io import BytesIO
import pyarrow.parquet as pq

GENERIC_NOT_FOUND = f"""{{
    "RemoteException": {{
        "exception": "FileNotFoundException",
        "javaClassName": "java.io.FileNotFoundException",
        "message": "File does not exist: /foo/a.patch"
    }}
}}"""

def generic_file(size: int) -> str:
    return f'''{{
    "FileStatus": {{
        "accessTime"      : 0,
        "blockSize"       : 0,
        "group"           : "",
        "length"          : {size},
        "modificationTime": 0,
        "owner"           : "",
        "pathSuffix"      : "",
        "permission"      : "666",
        "replication"     : 1,
        "type"            : "FILE"
    }}
}}'''

app = FastAPI()
app.files = {}

@app.route("/{full_path:path}")
async def all(request: Request, full_path: str):
    print(f"full path {full_path}")
    print(f"query param {request.query_params}")
    if request.query_params['op'] == "GETFILESTATUS":
        if full_path in app.files:
            return Response(generic_file(app.files[full_path]), 200, media_type='application/json')
        else:
            return Response(GENERIC_NOT_FOUND, 404, media_type='application/json')
    if request.query_params['op'] == "MKDIRS":
        return Response('{"boolean": true}', 200, media_type='application/json')
    if request.query_params["op"] == "CREATE":
        if "create_redirected" not in request.query_params:
            return Response(status_code=307, headers={"location": f"http://{request.base_url.host}:{request.base_url.port}/{full_path}?{request.query_params}&create_redirected=true"})
        else:
            # consume the body and asserts it's empty
            assert await request.body() == 0
            app.files[full_path] = 0
            return Response(status_code=201, headers={"location": f"hdfs://{request.base_url.host}:{request.base_url.port}/{full_path}"})
    if request.query_params["op"] in {"DELETE", "TRUNCATE"}:
        return Response('{"boolean": true}', 200, media_type='application/json')
    if request.query_params["op"] == "APPEND":
        if "append_redirected" not in request.query_params:
            return Response(status_code=307, headers={"location": f"http://{request.base_url.host}:{request.base_url.port}/{full_path}?{request.query_params}&append_redirected=true"})
        else:
            ret = await request.body()
            b = BytesIO(ret)
            app.files[full_path] += len(ret)
            results.append(pq.read_table(b))
            return Response(status_code=200)

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
