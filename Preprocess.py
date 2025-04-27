import pandas

df1 = pandas.read_csv("比賽用資料/Train/(Train)ACCTS_Data_202412.csv")
df2 = pandas.read_csv("比賽用資料/Train/(Train)ECCUS_Data_202412.csv")
df3 = pandas.read_csv("比賽用資料/Train/(Train)ID_Data_202412.csv")
df4 = pandas.read_csv("比賽用資料/Train/(Train)SAV_TXN_Data_202412.csv")

df = pandas.merge(df1, df2, on = ["ACCT_NBR", "CUST_ID"], how = "outer")
df = pandas.merge(df, df3, on = ["CUST_ID"], how = "outer")

df4_group = df4.groupby(["ACCT_NBR", "CUST_ID"])

df = pandas.merge(df, df4_group.size().reset_index(name = "TXN_COUNT"), how = "outer")
df = pandas.merge(df, df4_group["TX_AMT"].sum().reset_index(name = "TXN_AMT_SUM"), how = "outer")
df = pandas.merge(df, df4_group["TX_AMT"].mean().reset_index(name = "TXN_AMT_MEAN"), how = "outer")
df = pandas.merge(df, df4_group["eb_check"].mean().reset_index(name = "EB_CHECK_SUM"), how = "outer")
df = pandas.merge(df, df4_group["mb_check"].mean().reset_index(name = "MB_CHECK_SUM"), how = "outer")
df = pandas.merge(df, (df4_group["SAME_NUMBER_IP"].sum() / df4_group.size()).reset_index(name="IP_RATE"), how = "outer")
df = pandas.merge(df, (df4_group["SAME_NUMBER_UUID"].sum() / df4_group.size()).reset_index(name="UUID_RATE"), how = "outer")
df = pandas.merge(df, df4[df4["OWN_TRANS_ID"] == "ID99999"].groupby(["ACCT_NBR", "CUST_ID"]).size().reset_index(name="VIRTUAL_ID"), how = "outer")
df = pandas.merge(df, df4[df4["DRCR"] == 1].groupby(["ACCT_NBR", "CUST_ID"]).size().reset_index(name="DRCR_OUT"), how = "outer")
df = pandas.merge(df, df4[df4["DRCR"] == 2].groupby(["ACCT_NBR", "CUST_ID"]).size().reset_index(name="DRCR_IN"), how = "outer")
df4["TX_TIME"] = pandas.to_numeric(df4["TX_TIME"], errors='coerce')
df = pandas.merge(df, df4[((df4["TX_TIME"] >= 0) & (df4["TX_TIME"] <= 6))].groupby(["ACCT_NBR", "CUST_ID"]).size().reset_index(name="TX_TIME"), how = "outer")
channel_counts = df4.groupby(['ACCT_NBR', 'CUST_ID', 'CHANNEL_CODE']).size().reset_index(name='TX_COUNT')
max_channel = channel_counts.loc[channel_counts.groupby(['ACCT_NBR', 'CUST_ID'])['TX_COUNT'].idxmax()]
df = pandas.merge(df, max_channel.drop(columns = ["TX_COUNT"]), how = "outer")
trn_counts = df4.groupby(['ACCT_NBR', 'CUST_ID', 'TRN_CODE']).size().reset_index(name='TX_COUNT')
max_trn = trn_counts.loc[trn_counts.groupby(['ACCT_NBR', 'CUST_ID'])['TX_COUNT'].idxmax()]
df = pandas.merge(df, max_trn.drop(columns = ["TX_COUNT"]), how = "outer")

df['Y'] = df['DATA_DT'].notna().astype(int)
df = df.drop(columns = ["DATA_DT"])

df.to_csv('output.csv', index=False, encoding='utf-8')