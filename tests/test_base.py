import contextlib
import time
import threading
import uvicorn

import vertica_python as vp

from global_state import finish

i = 0

class Server(uvicorn.Server):
    def install_signal_handlers(self):
        pass

    @contextlib.contextmanager
    def run_in_thread(self):
        print('run_in_thread')
        thread = threading.Thread(target=self.run)
        print('Starting')
        thread.start()
        print('Thread started')
        try:
            while not self.started:
                print(f'Not started: {self.started}')
                time.sleep(1)
                if i > 30:
                    raise Exception('Server start failed')
                else:
                    i += 1
            yield
        finally:
            print('Exiting')
            self.should_exit = True
            thread.join()

print('Before config')
config = uvicorn.Config("fake_webhdfs:app", host="127.0.0.1", port=8000, log_level="trace")
print('Before server')
server = Server(config=config)
print('After server')

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
