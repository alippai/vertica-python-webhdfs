import contextlib
import time
import threading
import uvicorn

import pyarrow as pa
import pyarrow.parquet as pq
import vertica_python as vp

from fake_webhdfs import app

class Server(uvicorn.Server):
    def install_signal_handlers(self):
        pass

    @contextlib.contextmanager
    def run_in_thread(self):
        thread = threading.Thread(target=self.run)
        thread.start()
        try:
            while not self.started:
                time.sleep(0.01)
            yield
        finally:
            self.should_exit = True
            thread.join()

config = uvicorn.Config(app, host="127.0.0.1", log_level="trace")
server = Server(config=config)

def test_read_main():
    connection = vp.connect(
        **{"host": "127.0.0.1", "port": 5433, "user": "dbadmin", "database": "VMart"}
    )
    
    with server.run_in_thread():
        connection.cursor().execute(
            f"EXPORT TO PARQUET(directory = 'hdfs://127.0.0.1:{server.servers[0].sockets[0].getsockname()[1]}/virtualdata') AS SELECT 1 AS account_id"
        ).fetchall()
    
    # a single pyarrow table
    t = pa.concat_tables([pq.read_table(r) for r in app.state.results])
    print(t)
    assert t.equals(pa.Table.from_arrays([pa.array([1])], names=['account_id']))
