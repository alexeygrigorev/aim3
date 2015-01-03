import numpy as np
import pandas as pd

A = np.random.randint(100, size=(100, 100))
B = np.random.randint(100, size=(100, 100))
AB = A.dot(B)

A = pd.DataFrame(A).stack().reset_index()
B = pd.DataFrame(B).stack().reset_index()
AB = pd.DataFrame(AB).stack().reset_index()

A.to_csv('A.csv')
B.to_csv('B.csv')
AB.to_csv('AB.csv')