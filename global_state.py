import pyarrow as pa
import pyarrow.parquet as pq


files = {}
results = []

def finish():
    global files
    global results

    t = pa.concat_tables([pq.read_table(r) for r in results])

    files = {}
    results = []
    
    return t
