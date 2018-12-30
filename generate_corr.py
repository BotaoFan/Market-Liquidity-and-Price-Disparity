import pandas as pd
import numpy as np
import os

def load_csv(path):
    raw_data={}
    files_list=[]
    for root,dirs,files in os.walk(path):
        files_list=files
    for file in files_list:
        #print file
        file_name=file.replace('.csv','')
        try:
            raw_data[file_name]=pd.read_csv(path+file_name+'.csv',index_col=0)
        except:
            pass
    return raw_data

def stack_date_stock_df(df):
    df_stack=pd.DataFrame(df.stack())
    df_stack.index.names=[None,None]
    return df_stack

if __name__=='__main__':
    this_file_path=os.getcwd()
    raw_data_path=this_file_path+'/../raw_data/'
    factors_data_path=this_file_path+'/../factors_data_monthly/'
    stocks_list_path=this_file_path+'/../stocks_list.xlsx'

    raw_data=load_csv(raw_data_path)
    factors_data=load_csv(factors_data_path)
    stocks_list=pd.read_excel(stocks_list_path,coding='utf-8')

    min_count=15
    start_date = '20050601'
    end_date = '20180731'
    date_index = pd.date_range(start_date, end_date, freq='D')
    year_month_index = pd.Series([str(y) + '-' + str(m) for y, m in zip(date_index.year, date_index.month)])
    year_month_index.drop_duplicates(inplace=True)

    # Generate A H mkt_cap_ard
    mkt_cap_ard_A = raw_data['mkt_cap_ard_A'].copy()
    mkt_cap_ard_H = raw_data['mkt_cap_ard_H'].copy()
    et = raw_data['eco_et'].copy()
    mkt_cap_ard_A.index = pd.to_datetime(mkt_cap_ard_A.index)
    mkt_cap_ard_H.index = pd.to_datetime(mkt_cap_ard_H.index)
    et.index = pd.to_datetime(et.index)
    result_df = pd.DataFrame([], index=year_month_index)
    for id, A_code, H_code in zip(stocks_list['id'], stocks_list['A_code'], stocks_list['H_code']):
        temp_df = pd.DataFrame([], index=date_index)
        temp_df['year_month'] = [str(y) + '-' + str(m) for y, m in zip(temp_df.index.year, temp_df.index.month)]
        temp_df['mkt_cap_ard_A'] = mkt_cap_ard_A[A_code]
        temp_df.loc[temp_df['mkt_cap_ard_A'] == 0, 'mkt_cap_ard_A'] = np.nan
        temp_df['mkt_cap_ard_A'] = np.log(temp_df['mkt_cap_ard_A'])
        temp_df.dropna(inplace=True)
        temp_df_grouped = temp_df.groupby('year_month')
        temp_result = temp_df_grouped['mkt_cap_ard_A'].agg(['mean', 'count'])
        temp_result.loc[temp_result['count'] < min_count, 'mean'] = np.nan
        result_df[str(id)] = pd.DataFrame(temp_result['mean'])
    factors_data['mkt_cap_ard_A'] = result_df

    result_df = pd.DataFrame([], index=year_month_index)
    for id, A_code, H_code in zip(stocks_list['id'], stocks_list['A_code'], stocks_list['H_code']):
        temp_df = pd.DataFrame([], index=date_index)
        temp_df['year_month'] = [str(y) + '-' + str(m) for y, m in zip(temp_df.index.year, temp_df.index.month)]
        temp_df['mkt_cap_ard_H'] = mkt_cap_ard_H[H_code]
        temp_df['et'] = et
        temp_df.loc[temp_df['mkt_cap_ard_H'] == 0, 'mkt_cap_ard_H'] = np.nan
        temp_df.loc[temp_df['et'] == 0, 'et'] = np.nan
        temp_df['mkt_cap_ard_H'] = np.log(temp_df['mkt_cap_ard_H'] / temp_df['et'])
        temp_df.dropna(inplace=True)
        temp_df_grouped = temp_df.groupby('year_month')
        temp_result = temp_df_grouped['mkt_cap_ard_H'].agg(['mean', 'count'])
        temp_result.loc[temp_result['count'] < min_count, 'mean'] = np.nan
        result_df[str(id)] = pd.DataFrame(temp_result['mean'])
    factors_data['mkt_cap_ard_H'] = result_df

    # Generate A H vol
    close_A = raw_data['close_A'].copy()
    close_H = raw_data['close_H'].copy()
    close_A.index = pd.to_datetime(close_A.index)
    close_H.index = pd.to_datetime(close_H.index)
    result_df = pd.DataFrame([], index=year_month_index)
    for id, A_code, H_code in zip(stocks_list['id'], stocks_list['A_code'], stocks_list['H_code']):
        temp_df = pd.DataFrame([], index=date_index)
        temp_df['year_month'] = [str(y) + '-' + str(m) for y, m in zip(temp_df.index.year, temp_df.index.month)]
        stock_close_A = close_A[A_code].dropna()
        return_A = stock_close_A / stock_close_A.shift(1) - 1
        temp_df['volatility_A'] = return_A.rolling(window=30, center=False).std()
        temp_df.dropna(inplace=True)
        if temp_df.shape[0] > 0:
            temp_df_grouped = temp_df.groupby('year_month')
            temp_result = temp_df_grouped['volatility_A'].agg(['count', 'mean'])
            temp_result.loc[temp_result['count'] < min_count, 'mean'] = np.nan
            result_df[str(id)] = pd.DataFrame(temp_result['mean'])
        else:
            result_df[str(id)] = np.nan
    factors_data['vol_A'] = result_df

    result_df = pd.DataFrame([], index=year_month_index)
    for id, A_code, H_code in zip(stocks_list['id'], stocks_list['A_code'], stocks_list['H_code']):
        temp_df = pd.DataFrame([], index=date_index)
        temp_df['year_month'] = [str(y) + '-' + str(m) for y, m in zip(temp_df.index.year, temp_df.index.month)]
        stock_close_H = close_H[H_code].dropna()
        return_H = stock_close_H / stock_close_H.shift(1) - 1
        temp_df['volatility_H'] = return_H.rolling(window=30, center=False).std()
        temp_df.dropna(inplace=True)
        if temp_df.shape[0] > 0:
            temp_df_grouped = temp_df.groupby('year_month')
            temp_result = temp_df_grouped['volatility_H'].agg(['count', 'mean'])
            temp_result.loc[temp_result['count'] < min_count, 'mean'] = np.nan
            result_df[str(id)] = pd.DataFrame(temp_result['mean'])
        else:
            result_df[str(id)] = np.nan
    factors_data['vol_H'] = result_df

    # Generate A H cap_float
    mkt_cap_float_A = raw_data['mkt_cap_float_A'].copy()
    mkt_cap_float_H = raw_data['mkt_cap_float_H'].copy()
    et = raw_data['eco_et'].copy()
    mkt_cap_float_A.index = pd.to_datetime(mkt_cap_float_A.index)
    mkt_cap_float_H.index = pd.to_datetime(mkt_cap_float_H.index)
    et.index = pd.to_datetime(et.index)
    result_df = pd.DataFrame([], index=year_month_index)
    for id, A_code, H_code in zip(stocks_list['id'], stocks_list['A_code'], stocks_list['H_code']):
        temp_df = pd.DataFrame([], index=date_index)
        temp_df['year_month'] = [str(y) + '-' + str(m) for y, m in zip(temp_df.index.year, temp_df.index.month)]
        temp_df['mkt_cap_float_A'] = mkt_cap_float_A[A_code]
        temp_df['log_mkt_cap_float_A'] = np.log(temp_df['mkt_cap_float_A'])
        temp_df.dropna(inplace=True)
        temp_df_grouped = temp_df.groupby('year_month')
        temp_result = temp_df_grouped['log_mkt_cap_float_A'].agg(['mean', 'count'])
        temp_result.loc[temp_result['count'] < min_count, 'mean'] = np.nan
        result_df[str(id)] = pd.DataFrame(temp_result['mean'])
    factors_data['mkt_cap_float_A'] = result_df

    result_df = pd.DataFrame([], index=year_month_index)
    for id, A_code, H_code in zip(stocks_list['id'], stocks_list['A_code'], stocks_list['H_code']):
        temp_df = pd.DataFrame([], index=date_index)
        temp_df['year_month'] = [str(y) + '-' + str(m) for y, m in zip(temp_df.index.year, temp_df.index.month)]
        temp_df['mkt_cap_float_H'] = mkt_cap_float_H[H_code]
        temp_df['et'] = et
        temp_df['log_mkt_cap_float_H'] = np.log(temp_df['mkt_cap_float_H'] / temp_df['et'])
        temp_df.dropna(inplace=True)
        temp_df_grouped = temp_df.groupby('year_month')
        temp_result = temp_df_grouped['log_mkt_cap_float_H'].agg(['mean', 'count'])
        temp_result.loc[temp_result['count'] < min_count, 'mean'] = np.nan
        result_df[str(id)] = pd.DataFrame(temp_result['mean'])
    factors_data['mkt_cap_float_H'] = result_df

    corr_data=pd.DataFrame(factors_data['discount_y'].copy().stack())
    corr_data.columns=['discount_y']
    factors_list=['amihud_measure_A_volume','amihud_measure_H_volume','amihud_turnover_A',
                  'amihud_turnover_H','amihud_constant_A_volume','amihud_constant_H_volume',
                  'mkt_cap_ard_A','mkt_cap_ard_H','vol_A','vol_H','mkt_cap_float_A','mkt_cap_float_H']
    for key in factors_list:
        temp_df=stack_date_stock_df(factors_data[key].copy())
        temp_df.columns=[key]
        corr_data=corr_data.merge(temp_df,left_index=True,right_index=True,how='left')

    factors_list.append('discount_y')
    corr_df_pearson={}
    year_month_index_corr=corr_data.index.get_level_values(0).drop_duplicates()
    for i in year_month_index_corr:
        corr_df_pearson[i]=corr_data.loc[i,:].corr(method='pearson')
    corr_df_spearman={}
    for i in year_month_index_corr:
        corr_df_spearman[i]=corr_data.loc[i,:].corr(method='spearman')

    n=len(factors_list)
    corr_mean_pearson=pd.DataFrame(np.zeros((n,n)),index=factors_list,columns=factors_list)
    for i in factors_list:
        for j in factors_list:
            temp_corr_list=[]
            for ym in year_month_index_corr:
                c=corr_df_pearson[ym].loc[i,j]
                if not np.isnan(c):
                    temp_corr_list.append(c)
            temp_corr_average=np.mean(temp_corr_list)
            corr_mean_pearson.loc[i,j]=temp_corr_average
    corr_mean_pearson.to_csv(this_file_path+'/corr_mean_pearson1.csv')

    corr_mean_spearman=pd.DataFrame(np.zeros((n,n)),index=factors_list,columns=factors_list)
    for i in factors_list:
        for j in factors_list:
            temp_corr_list=[]
            for ym in year_month_index_corr:
                c=corr_df_spearman[ym].loc[i,j]
                if not np.isnan(c):
                    temp_corr_list.append(c)
            temp_corr_average=np.mean(temp_corr_list)
            corr_mean_spearman.loc[i,j]=temp_corr_average
    corr_mean_spearman.to_csv(this_file_path+'/corr_mean_spearman1.csv')