from multiprocessing import Pool

def parallelR(token):
    try:
        d[token]
        pass
    except:
        return token
    
def parallelRemove(d, tokens, p):
    with Pool(processes = p) as pool:
        results = pool.map(parallelR, tokens)
        pool.close()
        pool.join()
    return results