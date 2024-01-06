from io import BytesIO

from fastapi import FastAPI, Request, Response
import pyarrow.parquet as pq

from global_state import files, results

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

@app.get("/{full_path:path}")
async def get_handler(request: Request, full_path: str) -> Response:
    print(f"full path {full_path}")
    print(f"query param {request.query_params}")
    if request.query_params['op'] == "GETFILESTATUS":
        if full_path in files:
            return Response(generic_file(files[full_path]), 200, media_type='application/json')
        else:
            return Response(GENERIC_NOT_FOUND, 404, media_type='application/json')

@app.put("/{full_path:path}")
async def put_handler(request: Request, full_path: str) -> Response:
    print(f"full path {full_path}")
    print(f"query param {request.query_params}")
    if request.query_params['op'] == "MKDIRS":
        return Response('{"boolean": true}', 200, media_type='application/json')
    if request.query_params["op"] == "CREATE":
        if "create_redirected" not in request.query_params:
            return Response(status_code=307, headers={"location": f"http://{request.base_url.host}:{request.base_url.port}/{full_path}?{request.query_params}&create_redirected=true"})
        else:
            # consume the body and asserts it's empty
            assert await request.body() == 0
            files[full_path] = 0
            return Response(status_code=201, headers={"location": f"hdfs://{request.base_url.host}:{request.base_url.port}/{full_path}"})
    if request.query_params["op"] == "RENAME":
        return Response('{"boolean": true}', 200, media_type='application/json')

@app.delete("/{full_path:path}")
async def delete_handler(request: Request, full_path: str) -> Response:
    print(f"full path {full_path}")
    print(f"query param {request.query_params}")
    if request.query_params["op"] == "DELETE":
        return Response('{"boolean": true}', 200, media_type='application/json')
    
@app.post("/{full_path:path}")
async def post_handler(request: Request, full_path: str) -> Response:
    print(f"full path {full_path}")
    print(f"query param {request.query_params}")
    if request.query_params["op"] == "TRUNCATE":
        return Response('{"boolean": true}', 200, media_type='application/json')
    if request.query_params["op"] == "APPEND":
        if "append_redirected" not in request.query_params:
            return Response(status_code=307, headers={"location": f"http://{request.base_url.host}:{request.base_url.port}/{full_path}?{request.query_params}&append_redirected=true"})
        else:
            ret = await request.body()
            b = BytesIO(ret)
            files[full_path] += len(ret)
            results.append(pq.read_table(b))
            return Response(status_code=200)
