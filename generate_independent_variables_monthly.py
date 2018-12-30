#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Author:Botao Fan

#Generate factor data based on raw data
import pandas as pd
import numpy as np
import os

def load_csv(path):
    raw_data={}
    files_list=[]
    for root,dirs,files in os.walk(path):
        files_list=files
    for file in files_list:
        file_name=file.replace('.csv','')
        try:
            raw_data[file_name]=pd.read_csv(path+file_name+'.csv',index_col=0)
        except:
            pass
    return raw_data

def save_dict_as_csv(dict,path=''):
    for key in dict:
        dict[key].to_csv(path+key+'.csv')

if __name__=="__main__":
    #Set global variables
    start_date='20050601'
    end_date='20180731'
    date_index=pd.date_range(start_date,end_date,freq='D')
    year_month_index = pd.Series([str(y) + '-' + str(m) for y, m in zip(date_index.year, date_index.month)])
    year_month_index.drop_duplicates(inplace=True)
    #For every monthly factors , if number of daily data is less than min_count, the factor in this month should be dropped
    min_count=15
    #factors_data contains result
    factors_data={}
    this_file_path = os.getcwd()

    #Load raw data into raw_data which is dictionary from csv files
    raw_data_path=this_file_path+'/../raw_data/'
    raw_data=load_csv(raw_data_path)
    #Load stocks_list which contains AH stocks' information
    stocks_list_path=this_file_path+'/../stocks_list.xlsx'
    stocks_list=pd.read_excel(stocks_list_path, coding='utf-8')

    #Generate discount_y
    #close_A is the dataframe whose index is date and column is stock contains daily close price in A market
    close_A = raw_data['close_A'].copy()
    #close_H is the dataframe whose index is date and column is stock contains daily close price in H market
    close_H = raw_data['close_H'].copy()
    #et contains daily number of Hong Kong dollars per unit of RMB
    et = raw_data['eco_et'].copy()
    #Convert dataframe's index into datetime from string
    close_A.index = pd.to_datetime(close_A.index)
    close_H.index = pd.to_datetime(close_H.index)
    et.index = pd.to_datetime(et.index)
    #result_df is a dataframe whose index is year-month(such as 2017-1) and columns are stocks' id
    #And result_df will contains dependent variable which is H share discount.
    result_df = pd.DataFrame([], index=year_month_index)
    for id, A_code, H_code in zip(stocks_list['id'], stocks_list['A_code'], stocks_list['H_code']):
        temp_df = pd.DataFrame([], index=date_index)
        temp_df['year_month']=[str(y)+'-'+str(m) for y,m in zip(temp_df.index.year,temp_df.index.month)]
        temp_df['close_A'],temp_df['close_H'],temp_df['et'] = close_A[A_code],close_H[H_code],et
        #To avoid get inf so if close_A or et equals to zero,it will be set nan
        temp_df[temp_df['close_A']==0]=np.nan
        temp_df[temp_df['et']==0]=np.nan
        temp_df['result'] = temp_df['close_H'] / temp_df['et'] / temp_df['close_A'] - 1
        temp_df.dropna(inplace=True)
        temp_df_grouped=temp_df.groupby('year_month')
        temp_result=temp_df_grouped['result'].agg(['count','mean'])
        temp_result.loc[temp_result['count']<min_count,'mean']=np.nan
        result_df[id] = pd.DataFrame(temp_result['mean'])
    factors_data['discount_y'] = result_df

    #Generate Amihud_measure using volume in A market
    close_A = raw_data['close_A'].copy()
    volume_A = raw_data['volume_A'].copy()
    close_A.index = pd.to_datetime(close_A.index)
    volume_A.index = pd.to_datetime(volume_A.index)
    return_abs_A = np.abs(close_A / close_A.shift(1) - 1)
    result_df = pd.DataFrame([], index=year_month_index)
    for id, A_code in zip(stocks_list['id'], stocks_list['A_code']):
        temp_df = pd.DataFrame([], index=date_index)
        temp_df['return_abs_A'], temp_df['volume_A'] = return_abs_A[A_code],volume_A[A_code]
        temp_df['volume_A'][temp_df['volume_A']==0]=np.nan
        temp_df['divid'] = temp_df['return_abs_A'] / temp_df['volume_A']
        temp_df['year_month'] = [str(y) + '-' + str(m) for y, m in zip(temp_df.index.year, temp_df.index.month)]
        temp_df.dropna(inplace=True)
        temp_df_grouped = temp_df.groupby(['year_month'])
        temp_result=temp_df_grouped['divid'].agg(['mean','count'])
        temp_result.loc[temp_result['count']<min_count,'mean']=np.nan
        result_df[id] = pd.DataFrame(temp_result['mean'])
    factors_data['amihud_measure_A_volume']=result_df

    #Generate Amihud_measure using volume in H market
    close_H = raw_data['close_H'].copy()
    volume_H = raw_data['volume_H'].copy()
    close_H.index = pd.to_datetime(close_H.index)
    volume_H.index = pd.to_datetime(volume_H.index)
    return_abs_H = np.abs(close_H / close_H.shift(1) - 1)
    result_df = pd.DataFrame([], index=year_month_index)
    for id, H_code in zip(stocks_list['id'], stocks_list['H_code']):
        temp_df = pd.DataFrame([], index=date_index)
        temp_df['return_abs_H'],temp_df['volume_H'] = return_abs_H[H_code],volume_H[H_code]
        temp_df['volume_H'][temp_df['volume_H']==0]=np.nan
        temp_df['divid'] = temp_df['return_abs_H'] / temp_df['volume_H']
        temp_df['year_month'] = [str(y) + '-' + str(m) for y, m in zip(temp_df.index.year, temp_df.index.month)]
        temp_df.dropna(inplace=True)
        temp_df_grouped = temp_df.groupby(['year_month'])
        temp_result=temp_df_grouped['divid'].agg(['mean','count'])
        temp_result.loc[temp_result['count']<min_count,'mean']=np.nan
        result_df[id] = pd.DataFrame(temp_result['mean'])
    factors_data['amihud_measure_H_volume']=result_df

    #Generate Amihud turnover based in A market
    close_A = raw_data['close_A'].copy()
    turn_A = raw_data['turn_A'].copy()
    close_A.index = pd.to_datetime(close_A.index)
    turn_A.index = pd.to_datetime(turn_A.index)
    return_abs_A = np.abs(close_A / close_A.shift(1) - 1)
    result_df = pd.DataFrame([], index=year_month_index)
    for id, A_code in zip(stocks_list['id'], stocks_list['A_code']):
        temp_df = pd.DataFrame([], index=date_index)
        temp_df['return_abs_A'],temp_df['turn_A'] = return_abs_A[A_code],turn_A[A_code]
        temp_df['turn_A'][temp_df['turn_A']==0]=np.nan
        temp_df['divid'] = temp_df['return_abs_A'] / temp_df['turn_A']
        temp_df['year_month'] = [str(y) + '-' + str(m) for y, m in zip(temp_df.index.year, temp_df.index.month)]
        temp_df.dropna(inplace=True)
        temp_df_grouped = temp_df.groupby(['year_month'])
        temp_result=temp_df_grouped['divid'].agg(['mean','count'])
        temp_result.loc[temp_result['count']<min_count,'mean']=np.nan
        result_df[id] = temp_result['mean']
    factors_data['amihud_turnover_A']=result_df

    #Generate Amihud turnover based in H market
    close_H = raw_data['close_H'].copy()
    turn_H = raw_data['turn_H'].copy()
    close_H.index = pd.to_datetime(close_H.index)
    turn_H.index = pd.to_datetime(turn_H.index)
    return_abs_H = np.abs(close_H / close_H.shift(1) - 1)
    result_df = pd.DataFrame([], index=year_month_index)
    for id, H_code in zip(stocks_list['id'], stocks_list['H_code']):
        temp_df = pd.DataFrame([], index=date_index)
        temp_df['return_abs_H'],temp_df['turn_H'] = return_abs_H[H_code],turn_H[H_code]
        temp_df['turn_H'][temp_df['turn_H']==0]=np.nan
        temp_df['divid'] = temp_df['return_abs_H'] / temp_df['turn_H']
        temp_df['year_month'] = [str(y) + '-' + str(m) for y, m in zip(temp_df.index.year, temp_df.index.month)]
        temp_df.dropna(inplace=True)
        temp_df_grouped = temp_df.groupby(['year_month'])
        temp_result=temp_df_grouped['divid'].agg(['mean','count'])
        temp_result.loc[temp_result['count']<min_count,'mean']=np.nan
        result_df[id] = temp_result['mean']
    factors_data['amihud_turnover_H']=result_df



    #Generate constant measure of Amihud using volume in A market
    volume_A = raw_data['volume_A'].copy()
    volume_A.index = pd.to_datetime(volume_A.index)
    result_df = pd.DataFrame([], index=year_month_index)
    for id, A_code in zip(stocks_list['id'], stocks_list['A_code']):
        temp_df = pd.DataFrame([], index=date_index)
        temp_df['volume_A'] = volume_A[A_code]
        temp_df['volume_A'][temp_df['volume_A']==0]=np.nan
        temp_df['divid'] = 1 / temp_df['volume_A']
        temp_df['year_month'] = [str(y) + '-' + str(m) for y, m in zip(temp_df.index.year, temp_df.index.month)]
        temp_df.dropna(inplace=True)
        temp_df_grouped = temp_df.groupby(['year_month'])
        temp_result=temp_df_grouped['divid'].agg(['mean','count'])
        temp_result.loc[temp_result['count']<min_count,'mean']=np.nan
        result_df[id] = temp_result['mean']
    factors_data['amihud_constant_A_volume']=result_df

    #Generate constant measure of Amihud using volume in H market
    volume_H = raw_data['volume_H'].copy()
    volume_H.index = pd.to_datetime(volume_H.index)
    result_df = pd.DataFrame([], index=year_month_index)
    for id, H_code in zip(stocks_list['id'], stocks_list['H_code']):
        temp_df = pd.DataFrame([], index=date_index)
        temp_df['volume_H'] = volume_H[H_code]
        temp_df['volume_H'][temp_df['volume_H']==0]=np.nan
        temp_df['divid'] = 1 / temp_df['volume_H']
        temp_df['year_month'] = [str(y) + '-' + str(m) for y, m in zip(temp_df.index.year, temp_df.index.month)]
        temp_df.dropna(inplace=True)
        temp_df_grouped = temp_df.groupby(['year_month'])
        temp_result=temp_df_grouped['divid'].agg(['mean','count'])
        temp_result.loc[temp_result['count']<min_count,'mean']=np.nan
        result_df[id] = temp_result['mean']
    factors_data['amihud_constant_H_volume']=result_df

    #Generate A/H share size
    mkt_cap_ard_A = raw_data['mkt_cap_ard_A'].copy()
    mkt_cap_ard_H = raw_data['mkt_cap_ard_H'].copy()
    et = raw_data['eco_et'].copy()
    mkt_cap_ard_A.index = pd.to_datetime(mkt_cap_ard_A.index)
    mkt_cap_ard_H.index = pd.to_datetime(mkt_cap_ard_H.index)
    et.index = pd.to_datetime(et.index)
    result_df = pd.DataFrame([], index=year_month_index)
    for id, A_code, H_code in zip(stocks_list['id'], stocks_list['A_code'], stocks_list['H_code']):
        temp_df = pd.DataFrame([], index=date_index)
        temp_df['year_month']=[str(y)+'-'+str(m) for y,m in zip(temp_df.index.year,temp_df.index.month)]
        temp_df['mkt_cap_ard_A'],temp_df['mkt_cap_ard_H'],temp_df['et'] = mkt_cap_ard_A[A_code],mkt_cap_ard_H[H_code],et
        temp_df.loc[temp_df['mkt_cap_ard_A']==0,'mkt_cap_ard_A'] = np.nan
        temp_df.loc[temp_df['mkt_cap_ard_H'] == 0, 'mkt_cap_ard_H'] = np.nan
        temp_df.loc[temp_df['et'] == 0, 'et'] = np.nan
        temp_df['mkt_cap_ard_A'] = np.log(temp_df['mkt_cap_ard_A'])
        temp_df['mkt_cap_ard_H'] = np.log(temp_df['mkt_cap_ard_H']/temp_df['et'])
        temp_df['result'] = temp_df['mkt_cap_ard_A']  / temp_df['mkt_cap_ard_H']
        temp_df.dropna(inplace=True)
        temp_df_grouped=temp_df.groupby('year_month')
        temp_result=temp_df_grouped['result'].agg(['mean','count'])
        temp_result.loc[temp_result['count']<min_count,'mean']=np.nan
        result_df[id] = pd.DataFrame(temp_result['mean'])
    factors_data['AH_share_size']=result_df

    #Generate A/H vol
    close_A = raw_data['close_A'].copy()
    close_H = raw_data['close_H'].copy()
    close_A.index = pd.to_datetime(close_A.index)
    close_H.index = pd.to_datetime(close_H.index)
    result_df = pd.DataFrame([], index=year_month_index)
    for id, A_code, H_code in zip(stocks_list['id'], stocks_list['A_code'], stocks_list['H_code']):
        temp_df = pd.DataFrame([], index=date_index)
        temp_df['year_month']=[str(y)+'-'+str(m) for y,m in zip(temp_df.index.year,temp_df.index.month)]
        stock_close_A = close_A[A_code].dropna()
        stock_close_H = close_H[H_code].dropna()
        return_A = stock_close_A / stock_close_A.shift(1) - 1
        return_H = stock_close_H / stock_close_H.shift(1) - 1
        temp_df['volatility_A'] = return_A.rolling(window=30,center=False).std()
        temp_df['volatility_H'] = return_H.rolling(window=30,center=False).std()
        temp_df['volatility_AH'] = temp_df['volatility_A'] / temp_df['volatility_H']
        temp_df.dropna(inplace=True)
        if temp_df.shape[0]>0:
            temp_df_grouped=temp_df.groupby('year_month')
            temp_result=temp_df_grouped['volatility_AH'].agg(['count','mean'])
            temp_result.loc[temp_result['count']<min_count,'mean']=np.nan
            result_df[id] = pd.DataFrame(temp_result['mean'])
        else:
            result_df[id] = np.nan
    factors_data['vol']=result_df

    #Generate A/H supply
    mkt_cap_float_A = raw_data['mkt_cap_float_A'].copy()
    mkt_cap_float_H = raw_data['mkt_cap_float_H'].copy()
    et=raw_data['eco_et'].copy()
    mkt_cap_float_A.index = pd.to_datetime(mkt_cap_float_A.index)
    mkt_cap_float_H.index = pd.to_datetime(mkt_cap_float_H.index)
    et.index=pd.to_datetime(et.index)
    result_df = pd.DataFrame([], index=year_month_index)
    for id, A_code, H_code in zip(stocks_list['id'], stocks_list['A_code'], stocks_list['H_code']):
        temp_df = pd.DataFrame([], index=date_index)
        temp_df['year_month']=[str(y)+'-'+str(m) for y,m in zip(temp_df.index.year,temp_df.index.month)]
        temp_df['mkt_cap_float_A'],temp_df['mkt_cap_float_H'],temp_df['et'] = mkt_cap_float_A[A_code],mkt_cap_float_H[H_code],et
        temp_df['log_mkt_cap_float_A']=np.log(temp_df['mkt_cap_float_A'])
        temp_df['log_mkt_cap_float_H']=np.log(temp_df['mkt_cap_float_H']/temp_df['et'])
        temp_df['result'] = temp_df['log_mkt_cap_float_A'] / temp_df['log_mkt_cap_float_H']
        temp_df.dropna(inplace=True)
        temp_df_grouped=temp_df.groupby('year_month')
        temp_result=temp_df_grouped['result'].agg(['mean','count'])
        temp_result.loc[temp_result['count']<min_count,'mean']=np.nan
        result_df[id] = pd.DataFrame(temp_result['mean'])
    factors_data['supply'] = result_df

    #Generate index_CSI300/index_HSCEI
    index_CSI300 = raw_data['index_CSI300'].copy()
    index_HSCEI = raw_data['index_HSCEI'].copy()
    index_CSI300.index = pd.to_datetime(index_CSI300.index)
    index_HSCEI.index = pd.to_datetime(index_HSCEI.index)
    result_df = pd.DataFrame([], index=year_month_index)
    temp_df = pd.DataFrame([], index=date_index)
    temp_df['year_month'] = [str(y) + '-' + str(m) for y, m in zip(temp_df.index.year, temp_df.index.month)]
    temp_df['result']= index_CSI300 / index_HSCEI
    temp_df.dropna(inplace=True)
    temp_df_grouped=temp_df.groupby('year_month')
    temp_result=temp_df_grouped['result'].agg(['mean','count'])
    temp_result.loc[temp_result['count']<min_count,'mean']=np.nan
    result_df= pd.DataFrame(temp_result['mean'])
    factors_data['csi_hsc'] = result_df


    #Generate EXCH=et(t)/et(t-1)-1
    et=raw_data['eco_et'].copy()
    et.columns=['et']
    et.index=pd.to_datetime(et.index)
    et['year_month'] = [str(y) + '-' + str(m) for y, m in zip(et.index.year, et.index.month)]
    et.dropna(inplace=True)
    et_grouped=et.groupby('year_month').apply(lambda x: x[x.index==x.index.max()])
    et_grouped.index.rename(['year_month', 'date'], inplace=True)
    et_grouped.reset_index(level=['date'], inplace=True)
    exch=et_grouped['et']/et_grouped['et'].shift(1)-1
    factors_data['exch']=exch

    #Generate rate differential
    rate_differential=pd.DataFrame([],index=year_month_index)
    eco_RMB_interest_rate=raw_data['eco_RMB_interest_rate'].copy()
    eco_RMB_interest_rate.columns=['eco_RMB_interest_rate']
    eco_RMB_interest_rate.index= pd.to_datetime(eco_RMB_interest_rate.index)
    eco_RMB_interest_rate['year_month'] = [str(y) + '-' + str(m)
                                           for y, m in zip(eco_RMB_interest_rate.index.year, eco_RMB_interest_rate.index.month)]
    eco_RMB_interest_rate.set_index(['year_month'],inplace=True)

    eco_HKD_interest_rate=raw_data['eco_HKD_interest_rate'].copy()
    eco_HKD_interest_rate.columns=['eco_HKD_interest_rate']
    eco_HKD_interest_rate.index= pd.to_datetime(eco_HKD_interest_rate.index)
    eco_HKD_interest_rate['year_month'] = [str(y) + '-' + str(m)
                                           for y, m in zip(eco_HKD_interest_rate.index.year, eco_HKD_interest_rate.index.month)]
    eco_HKD_interest_rate.set_index(['year_month'],inplace=True)
    rate_differential['eco_RMB_interest_rate']=eco_RMB_interest_rate
    rate_differential['eco_HKD_interest_rate']=eco_HKD_interest_rate
    rate_differential['diff']=rate_differential['eco_RMB_interest_rate']/rate_differential['eco_HKD_interest_rate']
    rate_differential.dropna(inplace=True)
    factors_data['rate_differential']=rate_differential['diff']

    #Generate M2 growth
    m2=raw_data['eco_M2'].copy()
    m2.columns=['eco_M2']
    m2.dropna(inplace=True)
    m2.index=pd.to_datetime(m2.index)
    m2_growth=np.log(m2)-np.log(m2.shift(1))
    m2_growth['year_month'] = [str(y) + '-' + str(m) for y, m in zip(m2_growth.index.year, m2_growth.index.month)]
    m2_growth.set_index('year_month',inplace=True)
    factors_data['m2_growth']=m2_growth

    # Generate export_import
    eco_export = raw_data['eco_amount_of_export_China'].copy()
    eco_import = raw_data['eco_amount_of_import_China'].copy()
    eco_export.index = pd.to_datetime(eco_export.index)
    eco_import.index = pd.to_datetime(eco_import.index)
    temp_df = pd.DataFrame([], index=date_index)
    temp_df['year_month'] = [str(y) + '-' + str(m) for y, m in zip(temp_df.index.year, temp_df.index.month)]
    temp_df['result']= np.log(eco_export)-np.log(eco_import)
    temp_df.dropna(inplace=True)
    temp_df.set_index('year_month',inplace=True)
    factors_data['export_import'] = temp_df

    #Save as csv
    save_dict_as_csv(factors_data,this_file_path+'/../factor_test/')

