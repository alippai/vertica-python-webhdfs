from io import BytesIO
from typing import Any

from starlette.applications import Starlette
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from starlette.requests import Request

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

async def handler(request: Request) -> Response:
    full_path = request.url.path[1:]
    operation = request.query_params['op']
    if operation == "GETFILESTATUS":
        if full_path in app.state.files:
            return JSONResponse(generic_file(app.state.files[full_path]))
        else:
            return JSONResponse(GENERIC_NOT_FOUND, 404)
    if operation == "CREATE":
        if "create_redirected" not in request.query_params:
            return Response(status_code=307, headers={"location": f"http://{request.base_url.hostname}:{request.base_url.port}/{full_path}?{request.query_params}&create_redirected=true"})
        else:
            # consume the body and assert it's empty
            assert len(await request.body()) == 0
            app.state.files[full_path] = 0
            return Response(status_code=201, headers={"location": f"hdfs://{request.base_url.hostname}:{request.base_url.port}/{full_path}"})
    if operation in {"RENAME","DELETE", "TRUNCATE", "MKDIRS"}:
        return JSONResponse({"boolean": True})
    if operation == "APPEND":
        if "append_redirected" not in request.query_params:
            return Response(status_code=307, headers={"location": f"http://{request.base_url.hostname}:{request.base_url.port}/{full_path}?{request.query_params}&append_redirected=true"})
        else:
            ret = await request.body()
            b = BytesIO(ret)
            app.state.files[full_path] += len(ret)
            app.state.results.append(b)
            return Response()
    raise Exception(f"Unhandled operation {operation} at path {full_path} and method {request.method}")
app = Starlette(debug=True, routes=[Route("/{full_path:path}", handler, methods=["GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "OPTIONS", "TRACE", "PATCH"])])
