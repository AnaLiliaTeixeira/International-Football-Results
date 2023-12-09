import pandas as pd

df_goalscores = pd.read_csv('bd_dataset/goalscorers.csv')
df_goalscores.to_json("bd_json_files/goalscorers.json",orient = "records", date_format = "epoch", double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None, indent=2)

df_shootouts = pd.read_csv('bd_dataset/shootouts.csv')
df_shootouts.to_json("bd_json_files/shootouts.json",orient = "records", date_format = "epoch", double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None, indent=2)

df_results = pd.read_csv('bd_dataset/results.csv')
df_results.to_json("bd_json_files/results.json",orient = "records", date_format = "epoch", double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None, indent=2)