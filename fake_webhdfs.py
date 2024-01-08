from io import BytesIO
from typing import Any

from starlette.applications import Starlette
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from starlette.requests import Request

from global_state import files, results

GENERIC_NOT_FOUND = {
    "RemoteException": {
        "exception": "FileNotFoundException",
        "javaClassName": "java.io.FileNotFoundException",
        "message": "File does not exist: /foo/a.patch",
    }
}

def generic_file(size: int) -> dict[str, dict[str, Any]]:
    return {
        "FileStatus": {
            "accessTime"      : 0,
            "blockSize"       : 0,
            "group"           : "",
            "length"          : size,
            "modificationTime": 0,
            "owner"           : "",
            "pathSuffix"      : "",
            "permission"      : "666",
            "replication"     : 1,
            "type"            : "FILE",
        }
    }

async def get_handler(request: Request) -> Response:
    full_path = request.url.path[1:]
    print(f"full path {full_path}")
    print(f"query param {request.query_params}")
    if request.query_params['op'] == "GETFILESTATUS":
        if full_path in files:
            return JSONResponse(generic_file(files[full_path]))
        else:
            return JSONResponse(GENERIC_NOT_FOUND, 404)

async def put_handler(request: Request) -> Response:
    full_path = request.url.path[1:]
    print(f"full path {full_path}")
    print(f"query param {request.query_params}")
    if request.query_params['op'] == "MKDIRS":
        return JSONResponse({"boolean": True})
    if request.query_params["op"] == "CREATE":
        if "create_redirected" not in request.query_params:
            return Response(status_code=307, headers={"location": f"http://{request.base_url.hostname}:{request.base_url.port}/{full_path}?{request.query_params}&create_redirected=true"})
        else:
            # consume the body and assert it's empty
            assert len(await request.body()) == 0
            files[full_path] = 0
            return Response(status_code=201, headers={"location": f"hdfs://{request.base_url.hostname}:{request.base_url.port}/{full_path}"})
    if request.query_params["op"] == "RENAME":
        return JSONResponse({"boolean": True})

async def delete_handler(request: Request) -> Response:
    full_path = request.url.path[1:]
    print(f"full path {full_path}")
    print(f"query param {request.query_params}")
    if request.query_params["op"] == "DELETE":
        return JSONResponse({"boolean": True})
    
async def post_handler(request: Request) -> Response:
    full_path = request.url.path[1:]
    print(f"full path {full_path}")
    print(f"query param {request.query_params}")
    if request.query_params["op"] == "TRUNCATE":
        return JSONResponse({"boolean": True})
    if request.query_params["op"] == "APPEND":
        if "append_redirected" not in request.query_params:
            return Response(status_code=307, headers={"location": f"http://{request.base_url.hostname}:{request.base_url.port}/{full_path}?{request.query_params}&append_redirected=true"})
        else:
            ret = await request.body()
            b = BytesIO(ret)
            files[full_path] += len(ret)
            results.append(b)
            return Response()

print('App init')
app = Starlette(debug=True, routes=[
    Route("/{full_path:path}", get_handler, methods=["GET"]),
    Route("/{full_path:path}", put_handler, methods=["PUT"]),
    Route("/{full_path:path}", delete_handler, methods=["DELETE"]),
    Route("/{full_path:path}", post_handler, methods=["POST"]),
])
print('App initialized')
