import pandas as pd
import matplotlib.pyplot as plt


data = pd.read_csv('./before_optimization/performance_mysql_bo.csv')

mean_values = data.mean()  

plt.figure(figsize=(10, 6))
plt.boxplot(data.values)

plt.xlabel('Queries')
plt.ylabel('Tempo de Execução (s)')
plt.title('Box Plot dos Tempos de Execução das Queries com Médias, antes da otimização da DB, MySQL')

consulta_labels = list(data.columns)
for i, mean_value in enumerate(mean_values):
    plt.text(i + 1, mean_value + 0.01, f'{mean_value:.5f}', ha='center', va='bottom', fontsize=8, color='red')

plt.xticks(range(1, len(consulta_labels) + 1), consulta_labels, rotation=45, ha='right')

plt.tight_layout() 
plt.show()

data = pd.read_csv('./after_optimization/performance_mysql_ao.csv')

mean_values = data.mean() 

plt.figure(figsize=(10, 6))
plt.boxplot(data.values)

plt.xlabel('Queries')
plt.ylabel('Tempo de Execução (s)')
plt.title('Box Plot dos Tempos de Execução das Queries com Médias, depois da otimização da DB, MySQL')

consulta_labels = list(data.columns)  
for i, mean_value in enumerate(mean_values):
    plt.text(i + 1, mean_value + 0.01, f'{mean_value:.5f}', ha='center', va='bottom', fontsize=8, color='red')

plt.xticks(range(1, len(consulta_labels) + 1), consulta_labels, rotation=45, ha='right')

plt.tight_layout()
plt.show()
