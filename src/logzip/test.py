import pandas as pd
import random
import time

number = 2000
value = random.randint(1, number)

matrix = []
for i in range(6000000):
    row = [j for j in range(5)]
    row.append(value)
    matrix.append(row)
    


df = pd.DataFrame(matrix, columns=["l"+str(i) for i in range(6)])


t1 = time.time()
for eid in df["l5"].unique():
    subdf = df.loc[df["l5"] == eid] 
t2 = time.time()
print(t2-t1)