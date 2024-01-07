import contextlib
import time
import threading
import uvicorn

import vertica_python as vp

from global_state import finish

class Server(uvicorn.Server):
    def install_signal_handlers(self):
        pass

    @contextlib.contextmanager
    def run_in_thread(self):
        thread = threading.Thread(target=self.run)
        print('Starting')
        thread.start()
        print('Thread started')
        try:
            while not self.started:
                print(f'Not started: {self.started}')
                time.sleep(1)
            yield
        finally:
            print('Exiting')
            self.should_exit = True
            thread.join()

config = uvicorn.Config("fake_webhdfs:app", host="127.0.0.1", port=8000, log_level="trace")
server = Server(config=config)

def test_read_main():
    connection = vp.connect(
        **{"host": "127.0.0.1", "port": 5433, "user": "dbadmin", "database": "VMart"}
    )
    
    with server.run_in_thread():
        connection.cursor().execute(
            "EXPORT TO PARQUET(directory = 'hdfs://127.0.0.1:8000/virtualdata') AS SELECT 1 AS account_id"
        ).fetchall()
    
    # a single pyarrow table
    t = finish()
    print(t)
