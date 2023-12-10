import subprocess
import pandas as pd
import json
from pymongo import MongoClient
from json_files_creation import *
import matplotlib.pyplot as plt

with open('after_optimization/performance_mongo_ao.csv', 'w') as querys_archive:
    querys_archive.write("Simple Query1, Simple Query2, ComplexQuery1, ComplexQuery2, Simple Query1 indexing, Simple Query2 indexing, ComplexQuery1 indexing, ComplexQuery2 indexing\n")

for i in range(30):
    print("Iteration number:", (i+1))
    subprocess.run(['python3', 'after_optimization/mongo_creation_op.py'])
    subprocess.run(['python3', 'after_optimization/mongo_indexes_op.py'])

# # Lendo os dados do arquivo CSV para um DataFrame do Pandas
# data = pd.read_csv('./after_optimization/performance_mongo_ao.csv')


# # Calcular as médias de execução de cada consulta
# mean_values = data.mean()  # Calcula as médias de cada coluna

# # Criar um box plot
# plt.figure(figsize=(10, 6))  # Definir o tamanho da figura
# plt.boxplot(data.values)   # Criar o box plot

# # Adicionar rótulos aos eixos
# plt.xlabel('Consultas')
# plt.ylabel('Tempo de Execução (s)')
# plt.title('Box Plot dos Tempos de Execução das Consultas com Médias')

# # Adicionar a média de cada consulta acima do gráfico
# consulta_labels = list(data.columns)  # Obtém os rótulos das consultas
# for i, mean_value in enumerate(mean_values):
#     plt.text(i + 1, mean_value + 0.01, f'{mean_value:.5f}', ha='center', va='bottom', fontsize=8, color='red')

# # Adicionar rótulos às barras (opcional, se quiser identificar cada consulta)
# plt.xticks(range(1, len(consulta_labels) + 1), consulta_labels, rotation=45, ha='right')

# # Mostrar o box plot
# plt.tight_layout()  # Ajustar layout
# plt.show()