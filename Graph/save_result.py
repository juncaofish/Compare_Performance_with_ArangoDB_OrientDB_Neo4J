import pandas as pd   
a = range(10)
b = [item*2 for item in a]
data = pd.DateFrame({"idx":a, "value":b})
data.to_csv("result.csv", sep=",")