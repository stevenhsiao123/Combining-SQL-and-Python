import pyodbc  # 讀進 DB 登入紀錄、使用紀錄
import numpy as np
import pandas as pd
import datetime as dt
import glob
import sys

##### 1. 起始畫面
now = dt.datetime.now().hour
if 5 < now < 12:
    print('============================================\n早安，我的朋友，歡迎使用帳號使用紀錄調閱工具\n============================================\n')
elif 12 <= now < 18:
    print('============================================\n午安，我的朋友，歡迎使用帳號使用紀錄調閱工具\n============================================\n')
else:
    print('============================================\n晚安，我的朋友，歡迎使用帳號使用紀錄調閱工具\n============================================\n')

print('''【介紹】
本程式是為了讓分析師更有效率的能調閱客戶帳號的使用紀錄，
原先需透過 SQL 程式分次執行再複製到 excel，
本程式可一次可打包包含「查詢紀錄」、「登入紀錄」與「主題變更紀錄」，同時還有簡易的分析，連拉樞紐的時間都可以省去。

【注意事項】
使用前，請先確定你已經連上內湖VPN，並確定資料夾當中有 log_name.xlsx 這個檔案。
另外請注意，本程式的所有功能皆會排除分析師帳號(eland)，以及日報排程的模組，
如需了解詳細排除項目與使用方法請閱讀 Heaven 資料夾中的使用說明。

如果使用過程出現問題，或是有任何建議回饋，請回報給 AS2 的 Chris (#532)。\n
============================================\n''')

##### 2. 檔案檢查
print('>>> 正在檢查 log_name.xlsx 是否存在...', end='')
sys.stdout.flush()
if glob.glob('data/log_name.xlsx'):
    print('檔案存在！')
else:
    print('檔案不存在！請至 Heaven 位址中重新下載。')
    sys.exit(0)

##### 3. 輸入查詢帳號與查詢期間
# acc_txt = "\'EYIMD\'"
# acc_txt = "\'AN_HK_MQ\', \'AN_HAKU_MQ\', \'DP_BQ_RE\', \'DP_BQ\', \'LM\', \'LM_NARS\', \'IPSA\', \'IPSA_CPB\', \'ZA_ETS\', \'OC_ETS\', \'CPB\', \'NARS_BPI\', \'SENKA\'"
# start_time = '2020-09-01'
# end_time = '2020-09-18'

acc_txt = "\'" + str(input('>>> 請輸入想要調閱的帳號名稱，輸入完畢後請按 enter 鍵：')) + "\'"

while True:
    start_time = str(input('>>> 請輸入調閱起始時間 (ex: 2020-09-21)：'))
    if len(start_time) == 10:
        break
    else:
        print('你似乎輸入錯誤的格式，請重新輸入！')

while True:
    end_time = str(input('>>> 請輸入調閱結束時間 (ex: 2020-09-21)：'))
    if len(end_time) == 10:
        break
    else:
        print('你似乎輸入錯誤的格式，請重新輸入！')

##### 4. 開始調閱
print('\n>>> 開始進行調閱，服務帳號：%s，調閱期間為 %s 到 %s。\n' %(acc_txt, start_time, end_time))

print('>>> 正在讀取 log_name 檔案...', end='')
sys.stdout.flush()
log_name = pd.read_excel('data/log_name.xlsx')  # 模組對應名稱(log_name)
print('Done!')

# SQL 指令
print('>>> 正在連線 DB ...', end='')
sys.stdout.flush()
conn = pyodbc.connect('Driver={SQL Server};Server=10.20.20.1;Database=opv_general;UID=asap;PWD=asap1234')  # 串 DB 資料庫（記得先連內湖）
print('Done!')

# 4.4 版查詢使用紀錄
sql_1 = """SELECT *
            FROM opv_general.dbo.api_search_log
            WHERE service_account in (%s)  --填入服務帳號
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

# 4.3 版查詢使用紀錄
sql_2 = """SELECT *
            FROM opv_general.dbo.search_log
            WHERE service_account in (%s)  --填入服務帳號
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

# 登入紀錄
sql_3 = """SELECT *
            FROM opv_general.dbo.system_login_log
            WHERE service_account in (%s)  --填入服務帳號
            AND created_time >= '%s'
            AND created_time < '%s'
            AND status ='成功'
            AND user_account!= 'eland';""" % (acc_txt, start_time, end_time)

# 客戶編號
sql_4 = """SELECT * FROM dbo.service_account_history
           WHERE account in (%s)""" % (acc_txt)

print('>>> 正在查詢客戶編號 ...', end='')
sys.stdout.flush()
service_account_history = pd.read_sql(sql_4, conn)
service_id_list = service_account_history[['service_account_id', 'account']].drop_duplicates('service_account_id') # 客戶編號對應表

service_id = ', '.join(map(str, service_id_list['service_account_id'].to_list()))

print('Done!')

# 主題變更紀錄
sql_5 = """SELECT * FROM dbo.user_profile_history
           WHERE f_account in (%s)
           AND update_time >= '%s'
           AND update_time < '%s'""" % (service_id, start_time, end_time)

# 執行 SQL 指令
print('>>> 正在查詢 Query 紀錄 ...', end='')
sys.stdout.flush()
df_query_1 = pd.read_sql(sql_1, conn)
df_query_2 = pd.read_sql(sql_2, conn)

query = pd.concat([df_query_1, df_query_2])
query.reset_index(drop=True, inplace=True)
print('Done!')

print('>>> 正在查詢 Login 紀錄 ...', end='')
sys.stdout.flush()
login = pd.read_sql(sql_3, conn)
print('Done!')

print('>>> 正在查詢 Changelog 紀錄 ...', end='')
sys.stdout.flush()
changelog = pd.read_sql(sql_5, conn)
print('Done!')

##### 5. 資料處理
print('>>> 正在進行查詢紀錄處理 ...', end='')
sys.stdout.flush()
query = query.merge(log_name, how='left', left_on='module_name', right_on='代號')   # 模組名稱取代成中文
query.drop('代號', axis=1, inplace=True)
query.loc[query['功能名稱'] == '觀測-關鍵字預覽', 'topic_name'] = '關鍵字預覽(無主題)'  # 關鍵字預覽無主題取代
query['topic_name'].replace('', '無主題查詢', inplace=True)  # 空白主題取代成「無主題查詢」
query['create_time'] = pd.to_datetime(query['create_time'])
query = query[(query['create_time'].dt.hour >= 1) & (query['create_time'].dt.hour <= 4) == False]  # 移除 1~4 點的資料
query.loc[query['function_name'] == 'Dictionary', '功能名稱'] = '關鍵字助教'
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

##### 6. 簡易分析
# 通用樞紐工具
print('>>> 正在進行分析 ...', end='')
sys.stdout.flush()
query['年月'] = query['create_time'].dt.strftime('%Y-%m')
def top_n(name):
    output = query.groupby([name, '年月'])[name].count().unstack()
    output.reset_index(inplace=True)
    output.fillna(0, inplace=True)
    output['總計'] = output.sum(axis=1, numeric_only=True)
    output = output.sort_values(by=['總計'], ascending=False)
    return output

# 主題查詢排行
hot_topic = top_n('topic_name')

# 使用者查詢排行
hot_user = top_n('user_account')

# 功能使用排行
hot_func = top_n('功能名稱')
print('Done!')

##### 7. 儲存
print('>>> 正在儲存 ...')
sys.stdout.flush()
with pd.ExcelWriter('output/帳號使用紀錄_%s_%s-%s.xlsx' %(acc_txt, start_time.replace('-', ''), end_time.replace('-', '')), options={'strings_to_urls': False}) as writer:
    query.to_excel(writer, sheet_name='01 查詢紀錄', index=False)
    login.to_excel(writer, sheet_name='02 登入紀錄', index=False)
    changelog.to_excel(writer, sheet_name='03 主題變更紀錄', index=False)
    hot_topic.to_excel(writer, sheet_name='A. 主題查詢排行', index=False)
    hot_user.to_excel(writer, sheet_name='B. 使用者查詢排行', index=False)
    hot_func.to_excel(writer, sheet_name='C. 功能使用排行', index=False)
print('%s %s-%s 的紀錄已儲存。' %(acc_txt, start_time.replace('-', ''), end_time.replace('-', '')))

# import os
# import sys
# #mypath = os.getcwd() #目前工作目錄(Py的位置)
# if hasattr(sys, "frozen"):#呈現檔案所在位置
#     mypath = os.path.join(os.path.dirname(sys.executable))
# else:
#     mypath = os.path.join(os.path.dirname(__file__))
# #設定檔
# in_data = pd.read_excel( r''+ mypath + '\\設定\\xxx.xlsx' ,sheet_name=r"設定" ,encoding='utf-8')
# acc_txt = str(in_data[r'帳號'][0])
# start_time = str(in_data[r'請輸入調閱起始時間 (ex: 2020-09-21)：'][0])
# end_time = str(in_data[r'請輸入調閱結束時間 (ex: 2020-09-21)：'][0])
# log_name = pd.read_excel(r''+ mypath + '\\data\\log_name.xlsx' ,sheet_name=r"log_name" ,encoding='utf-8')  # 模組對應名稱(log_name)
# output = r''+ mypath + '\\output\\帳號使用紀錄_%s_%s-%s.xlsx'