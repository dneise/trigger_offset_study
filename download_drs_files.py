# coding: utf-8
from fact.credentials import create_factdb_engine
from tqdm import tqdm
import shlex
from subprocess import check_call
import pandas as pd

db = create_factdb_engine()

def path_from_row(r):
    n_str = str(r.fNight)
    yyyy = n_str[0:4]
    mm = n_str[4:6]
    dd = n_str[6:8]
    path = "isdc:/fact/raw/{yyyy}/{mm}/{dd}/{n_str}_{runid:03d}.drs.fits.gz".format(
        yyyy=yyyy,
        mm=mm,
        dd=dd,
        runid=r.fRunID,
        n_str=n_str
    )
    return path
df = pd.read_sql_query("""
SELECT * from RunInfo 
WHERE fDrsStep=2
AND fRunTypeKey=2""", db)

df = df.sample(frac=1).reset_index(drop=True)

for r in tqdm(df.itertuples()):
    p = path_from_row(r)
    try:
        check_call(shlex.split("scp {0} data/.".format(p)))
    except:
        pass

