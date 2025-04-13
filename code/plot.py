import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

df_lm = pd.read_csv("result/LM/result_with_excess.csv")
df_harvard = pd.read_csv("result/Harvard/result_with_excess.csv")

df_sorted_tf_idf_lm = df_lm.sort_values(by='tfidf_score', ascending=True)
df_sorted_tf_idf_harvard = df_harvard.sort_values(by='tfidf_score', ascending=True)

if 'term_weights' in df_lm.columns:
    df_sorted_term_weight_lm = df_lm.sort_values(by='term_weights', ascending=True)
if 'term_weights' in df_harvard.columns:
    df_sorted_term_weight_harvard = df_harvard.sort_values(by='term_weights', ascending=True)

df_lm['tf_idf_quintile'] = pd.qcut(df_lm['tfidf_score'], q=5, labels=["Low", "2", "3", "4", "High"])
df_harvard['tf_idf_quintile'] = pd.qcut(df_harvard['tfidf_score'], q=5, labels=["Low", "2", "3", "4", "High"])

df_lm['term_weights_quintile'] = pd.qcut(df_lm['term_weights'], q=5, labels=["Low", "2", "3", "4", "High"])
df_harvard['term_weights_quintile'] = pd.qcut(df_harvard['term_weights'], q=5, labels=["Low", "2", "3", "4", "High"])


quintile_medians_lm = df_lm.groupby('tf_idf_quintile').agg({
    'ret_4day': 'median',
    'ret_3day': 'median'
}).reset_index()

quintile_medians_harvard = df_harvard.groupby('tf_idf_quintile').agg({
    'ret_4day': 'median',
    'ret_3day': 'median'
}).reset_index()

plt.figure(figsize=(10, 6))
plt.plot(quintile_medians_lm['tf_idf_quintile'], quintile_medians_lm['ret_3day'], 
         label='Fin-Neg', marker='o', color='black', linestyle='-')
plt.plot(quintile_medians_harvard['tf_idf_quintile'], quintile_medians_harvard['ret_3day'], 
         label='H4N-Inf', marker='o', color='grey', linestyle='--')
plt.xlabel('TF-IDF Quintile')
plt.ylabel('Median 3-Day Excess Return')
plt.title('Median 3-Day Excess Returns by TF-IDF Quintile')
plt.legend()
plt.grid(True)
plt.show()

plt.figure(figsize=(10, 6))
plt.plot(quintile_medians_lm['tf_idf_quintile'], quintile_medians_lm['ret_4day'], 
         label='Fin-Neg', marker='o', color='black', linestyle='-')
plt.plot(quintile_medians_harvard['tf_idf_quintile'], quintile_medians_harvard['ret_4day'], 
         label='H4N-Inf', marker='o', color='grey', linestyle='--')
plt.xlabel('TF-IDF Quintile')
plt.ylabel('Median 4-Day Excess Return')
plt.title('Median 4-Day Excess Returns by TF-IDF Quintile')
plt.legend()
plt.grid(True)
plt.show()


quintile_medians_lm = df_lm.groupby('term_weights_quintile').agg({
    'ret_4day': 'median',
    'ret_3day': 'median'
}).reset_index()

quintile_medians_harvard = df_harvard.groupby('term_weights_quintile').agg({
    'ret_4day': 'median',
    'ret_3day': 'median'
}).reset_index()


plt.figure(figsize=(10, 6))
plt.plot(quintile_medians_lm['term_weights_quintile'], quintile_medians_lm['ret_3day'], 
         label='Fin-Neg', marker='o', color='black', linestyle='-')
plt.plot(quintile_medians_harvard['term_weights_quintile'], quintile_medians_harvard['ret_3day'], 
         label='H4N-Inf', marker='o', color='grey', linestyle='--')
plt.xlabel('Term Weights Quintile')
plt.ylabel('Median 3-Day Excess Return')
plt.title('Median 3-Day Excess Returns by Term Weights Quintile')
plt.legend()
plt.grid(True)
plt.show()


plt.figure(figsize=(10, 6))
plt.plot(quintile_medians_lm['term_weights_quintile'], quintile_medians_lm['ret_4day'], 
         label='Fin-Neg', marker='o', color='black', linestyle='-')
plt.plot(quintile_medians_harvard['term_weights_quintile'], quintile_medians_harvard['ret_4day'], 
         label='H4N-Inf', marker='o', color='grey', linestyle='--')
plt.xlabel('Term Weights Quintile')
plt.ylabel('Median 4-Day Excess Return')
plt.title('Median 4-Day Excess Returns by Term Weights Quintile')
plt.legend()
plt.grid(True)
plt.show()