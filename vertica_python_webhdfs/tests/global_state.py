import pyarrow as pa


files = {}
results = []

def finish():
    files = {}
    t = pa.concat_tables(results)
    results = []
    return t
