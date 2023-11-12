import pyodbc  # 
import numpy as np
import pandas as pd
import datetime as dt
import glob
import sys

##### 1. Starting page of .exe file
now = dt.datetime.now().hour
if 5 < now < 12:
    print('============================================\nGood morning, thank you for using this client usage report generator\n============================================\n')
elif 12 <= now < 18:
    print('============================================\nGood afternoon, thank you for using this client usage report generator\n============================================\n')
else:
    print('============================================\nGood evening, thank you for using this client usage report generator\n============================================\n')

print('''【Introduction】
The main aim of this toolkit is to let every users extract product usage data for each of our client efficiently and timely.

Formerly, you need to log into database to retrive data from different table respectively and copy those output to separate Excel sheets, finally creating pivot tables in Excel to get the desired data.

With this toolkit, you can do all these only with a few clicks!

【Please note】

1.Make sure you are connected to company's WIFI when using.
2.Make sure you have file "log_name.xlsx" in your folder.

============================================\n''')

##### 2. Check if file exists
print('>>> Checking if "log_name.xlsx exists"...', end='')
sys.stdout.flush()
if glob.glob('data/log_name.xlsx'):
    print('File exists！')
else:
    print('File does\'nt exist, please make sure you have it in the folder')
    sys.exit(0)

##### 3. Enter clien's service account and time you're looking for  

acc_txt = "\'" + str(input('>>> Please enter client\'s service account and press enter to continue')) + "\'"

while True:
    start_time = str(input('>>> Please enter the start date in YYYY-MM-DD format (ex: 2020-09-21) and press enter to continue'))
    if len(start_time) == 10:
        break
    else:
        print('It seems that wrong date format has been entered, please try again')

while True:
    end_time = str(input('>>> Please enter the end date in YYYY-MM-DD format (ex: 2020-09-21) and press enter to continue'))
    if len(end_time) == 10:
        break
    else:
        print('It seems that wrong date format has been entered, please try again')

##### 4. Start extracting
print('\n>>> Now retrieving usage report >>> Clien\'s service account：%s, Time: from %s to %s。\n' %(acc_txt, start_time, end_time))

print('>>> Now reading "log_name" file', end='')
sys.stdout.flush()
log_name = pd.read_excel('data/log_name.xlsx')  # English and Chinese function name lookup table (log_name)
print('Done!')

# SQL queries
print('>>> Connecting to database ...', end='')
sys.stdout.flush()
conn = pyodbc.connect('Driver={SQL Server};Server=10.20.20.1;Database=opv_general;UID=asap;PWD=asap1234')  # Connect to the database
print('Done!')

# Search history
sql_1 = """SELECT *
            FROM opv_general.dbo.api_search_log
            WHERE service_account in (%s)  --Enter service accont 
            AND module_name!='dailyreport'  --排除標準日報排程名稱
            AND module_name!='daily_report'  --排除標準日報排程名稱
            AND module_name!='EmailRealTimesReport'    --排除客製自動排程功能名稱
            AND module_name!='DailyReportOfUnipresident'  --排除客製自動排程功能名稱
            AND module_name!='urgentReport'  --排除客製自動排程功能名稱
            AND module_name!='weeklyUrgentReport'  --排除客製自動排程功能名稱
            AND module_name!='Notification'  --排除預警報查詢自動排程功能名稱
            AND create_time >= '%s' --開始查詢日期
            AND create_time < '%s' --結束查詢日期
            AND user_account!= 'eland' --排除分析師帳號
            ORDER by create_time ASC;  --查詢執行時間點由舊到新排序""" % (acc_txt, start_time, end_time)


# Login history
sql_2 = """SELECT *
            FROM opv_general.dbo.system_login_log
            WHERE service_account in (%s)  --Enter service accont
            AND created_time >= '%s'
            AND created_time < '%s'
            AND status ='成功'
            AND user_account!= 'eland';""" % (acc_txt, start_time, end_time)

# Get service account ID (In case some tables only have service account ID instead of service account name)
sql_4 = """SELECT * FROM dbo.service_account_history
           WHERE account in (%s)""" % (acc_txt)

print('>>> Now searching for service account ID ...', end='')
sys.stdout.flush()
service_account_history = pd.read_sql(sql_4, conn)
service_id = service_account_history['service_account_id'].drop_duplicates('service_account_id') # 客戶編號對應表

#service_id = ', '.join(map(str, service_id_list['service_account_id'].to_list()))


print('Done!')

# Topic edit history
sql_5 = """SELECT * FROM dbo.user_profile_history
           WHERE f_account in (%s)
           AND update_time >= '%s'
           AND update_time < '%s'""" % (service_id, start_time, end_time)

# Executing SQL queries
print('>>> Extracting search history ...', end='')
sys.stdout.flush()
df_query_1 = pd.read_sql(sql_1, conn)
print('Done!')

print('>>> Extracting login history ...', end='')
sys.stdout.flush()
login = pd.read_sql(sql_3, conn)
print('Done!')

print('>>> Extracting Topic edit history ...', end='')
sys.stdout.flush()
changelog = pd.read_sql(sql_5, conn)
print('Done!')

##### 5. Data processing
print('>>> Processing client usage report ...', end='')
sys.stdout.flush()
query = query.merge(log_name, how='left', left_on='module_name', right_on='English_finction_name')   # Replace english function name with Chinese function name
query.drop('English_finction_name', axis=1, inplace=True)
query.loc[query['function_name_Chinese'] == '觀測-關鍵字預覽', 'topic_name'] = '關鍵字預覽(無主題)'  # 關鍵字預覽無主題取代
query['topic_name'].replace('', '無主題查詢', inplace=True)  # 空白主題取代成「無主題查詢」
query['create_time'] = pd.to_datetime(query['create_time'])
query = query[(query['create_time'].dt.hour >= 1) & (query['create_time'].dt.hour <= 4) == False]  # 移除 1~4 點的資料
query.loc[query['function_name'] == 'Dictionary', 'function_name_Chinese'] = '關鍵字助教'
print('Done!')

print('>>> 正在進行登入紀錄處理 ...', end='')
sys.stdout.flush()
login.columns = ['id', '服務帳號', '使用者帳號', 'IP', '登入狀態', '登入時間']
print('Done!')

print('>>> 正在進行主題變更紀錄處理 ...', end='')
sys.stdout.flush()
changelog = changelog.merge(service_id_list, how='left', left_on='f_account', right_on='service_account_id') # 客戶編號與客戶服務帳號對應
changelog.drop(['name_keyword', 'name_display', 'service_account_id'], axis=1, inplace=True)
changelog.columns = ['id', 'account_id', '動作', '變更時間', 'edited_id', '編輯者帳號', '顯示名稱(變更前)', '顯示名稱(變更後)', '關鍵詞組(變更前)', '關鍵詞組(變更後)', '服務帳號']
print('Done!')

##### 6. Analysis
# Pivot table function
print('>>> Analysis in progress ...', end='')
sys.stdout.flush()
query['Year_month'] = query['create_time'].dt.strftime('%Y-%m')#Add column for analysis purpose
def top_n(name):
    output = query.groupby([name, 'Year_month'])[name].count().unstack()
    output.reset_index(inplace=True)
    output.fillna(0, inplace=True)
    output['Total_usage'] = output.sum(axis=1, numeric_only=True)
    output = output.sort_values(by=['Total_usage'], ascending=False)
    return output

# Most searched topic ranking analysis
hot_topic = top_n('topic_name')

# Most frequent user ranking analysis
hot_user = top_n('user_account')

# Most used function ranking analysis
hot_func = top_n('function_name_Chinese')
print('Done!')

##### 7. Save the output into excel file
print('>>> Saving ...')
sys.stdout.flush()
with pd.ExcelWriter('output/client_usage_report_%s_%s-%s.xlsx' %(acc_txt, start_time.replace('-', ''), end_time.replace('-', '')), options={'strings_to_urls': False}) as writer:
    query.to_excel(writer, sheet_name='01 Search history', index=False)
    login.to_excel(writer, sheet_name='02 Login history', index=False)
    changelog.to_excel(writer, sheet_name='03 Topic edit history', index=False)
    hot_topic.to_excel(writer, sheet_name='A. Topic ranking analysis', index=False)
    hot_user.to_excel(writer, sheet_name='B. Users ranking analysis', index=False)
    hot_func.to_excel(writer, sheet_name='C. Function ranking analysis', index=False)
print('%s %s-%s usage report has been saved。' %(acc_txt, start_time.replace('-', ''), end_time.replace('-', '')))
