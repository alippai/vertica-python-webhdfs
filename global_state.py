import pyarrow as pa


files = {}
results = []

def finish():
    global files
    global results

    t = pa.concat_tables(results)

    files = {}
    results = []
    
    return t
