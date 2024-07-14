# Databricks notebook source
import pandas as pd
import numpy as np
import random
import string

# データの生成
np.random.seed(0)
data = {
    'ID': range(1, 101),
    'Name': [random.choice(['John', 'Jane', 'Doe', None, '']) for _ in range(100)],
    'Age': np.random.randint(18, 60, size=100),
    'Email': [random.choice(['', 'invalid_email', None]) if random.random() < 0.1 else f"{random.choice(string.ascii_lowercase)}{random.randint(100,999)}@example.com" for _ in range(100)],
    'Salary': np.random.choice([None, 50000, 60000, 70000, 80000, 90000], size=100),
    'Country': [random.choice(['USA', 'Canada', 'UK', 'Australia', '', None]) for _ in range(100)],
    'RegistrationDate': pd.date_range(start='2020-01-01', periods=100, freq='D').to_series().dt.strftime('%Y-%m-%d').tolist()
}
df = pd.DataFrame(data)

# 欠損値をランダムに挿入
for col in ['Age', 'Salary']:
    df.loc[df.sample(frac=0.1).index, col] = np.nan

# データの一部を意図的に汚す
df.loc[::10, 'Name'] = ''  # 10行ごとに名前を空にする
df.loc[::20, 'Email'] = 'invalid_email'  # 20行ごとに無効なメールアドレスを挿入

# CSVファイルとして保存
df.to_csv('./dirty_data.csv', index=False)

print("サンプルデータが生成され、'./dirty_data.csv'に保存されました。")
