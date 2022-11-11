import pandas as pd
import os






import os
import sys
from tqdm import tqdm
fn = os.path.join('RawData','Inv_.csv')
temp = pd.read_csv(fn, nrows=20)
N = len(temp.to_csv(index=False))
t = int(os.path.getsize(fn)/N*20/10**5) + 1




with tqdm(total = t, file = sys.stdout) as pbar:
    for i,chunk in enumerate(pd.read_csv(fn, chunksize=10**5, low_memory=False)):
        # df.append(chunk)
        pbar.set_description('Importing: %d' % (1 + i))
        pbar.update(1)

 


