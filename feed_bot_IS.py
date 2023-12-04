import numpy as np
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta

import io
import telegram

import matplotlib.pyplot as plt
import seaborn as sns

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Default parameters to be sent to tasks
default_args = {
    'owner': 'il-smirnov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2023, 11, 13),
}

# Launch interval for DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def feed_report_bot():
    @task()
    def extract_data():
        # Parameters to access ClickHouse
        connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': 'dpo_python_2020',
            'user': 'student',
            'database': 'simulator'
        }
        
        q = """
            select toDate(time) date, uniqExact(user_id) DAU, countIf(action='view') views, 
            countIf(action='like') likes, countIf(action='like')/countIf(action='view') CTR
            from simulator_20231020.feed_actions
            where date between today() - 7 and yesterday()
            group by date
            order by date desc
            """
        
        df = ph.read_clickhouse(q, connection=connection)
        return df
    
    # Task to create a report message (text is in Russian)          
    @task()
    def text_message(df):
        metrics = ['DAU', 'views', 'likes', 'CTR']
        metrics_dict = {}
        for i in range(len(metrics)):
            metrics_dict[metrics[i]] = [df[metrics[i]].iloc[0].round(4), (100*(df[metrics[i]].iloc[0]/df[metrics[i]].iloc[1] - 1)).round(2)]
            
            print_metrics = 'Значение метрик за вчерашний день ({yesterday}): \n \n\
DAU = {DAU} ({DAU_diff}%) \n\
Лайки = {likes} ({likes_diff}%) \n\
Просмотры = {views} ({views_diff}%) \n\
CTR = {CTR} ({CTR_diff}%) \n \n\
В скобках указан относительный прирост по сравнению с предыдущим днем ({yesterday_1})'
        
        print_metrics = print_metrics.format(yesterday = df.date.iloc[0].date(), 
                     DAU = metrics_dict['DAU'][0], DAU_diff = metrics_dict['DAU'][1], 
                     likes = metrics_dict['likes'][0], likes_diff = metrics_dict['likes'][1],
                     views = metrics_dict['views'][0], views_diff = metrics_dict['views'][1],
                     CTR = metrics_dict['CTR'][0], CTR_diff = metrics_dict['CTR'][1],
                     yesterday_1 = df.date.iloc[1].date())
        return print_metrics
    
    # Task to plot the metrics (axes are in Russian)    
    @task()
    def plot_metrics(df):
        fig, axs = plt.subplots(2,2,figsize=(18,10))
        fig.suptitle('Основные метрики ленты новостей за последние 7 дней', fontsize=18)

        axs[0,0].plot(df.date, df.DAU, linewidth=3)
        axs[0,0].set_title('DAU', fontsize=14)
        axs[0,0].grid(axis='both', alpha=.3)
        axs[0,0].set_xlim([df.date.iloc[-1], df.date.iloc[0]])

        axs[0,1].plot(df.date, df.likes, linewidth=3)
        axs[0,1].set_title('Лайки', fontsize=14)
        axs[0,1].grid(axis='both', alpha=.3)
        axs[0,1].set_xlim([df.date.iloc[-1], df.date.iloc[0]])

        axs[1,0].plot(df.date, df.views, linewidth=3)
        axs[1,0].set_title('Просмотры', fontsize=14)
        axs[1,0].grid(axis='both', alpha=.3)
        axs[1,0].set_xlim([df.date.iloc[-1], df.date.iloc[0]])

        axs[1,1].plot(df.date, df.CTR, linewidth=3)
        axs[1,1].set_title('CTR', fontsize=14)
        axs[1,1].grid(axis='both', alpha=.3)
        axs[1,1].set_xlim([df.date.iloc[-1], df.date.iloc[0]])

        fig.tight_layout()
        plt.subplots_adjust(wspace=.2, hspace=.3)
        #fig.savefig('feed_report.png')

        plot_metrics = io.BytesIO() # create a file object
        plt.savefig(plot_metrics)
        plot_metrics.seek(0) # to move cursor to the begining of the object we just created
        plt.close()
        
        return plot_metrics
    
    # Task to send message and figures to our chat using tg-bot
    @task()
    def send_metrics(print_metrics, plot_metrics):
        #my_chat_id = 117705970
        chat_id = -4042591545
        my_token = '6384523160:AAHLGiud-LcmjE-COmJie_wcyXnxeuGw3r4'
        
        bot = telegram.Bot(token=my_token) # access to tg-bot
        bot.sendMessage(chat_id=chat_id, text=print_metrics)
        bot.sendPhoto(chat_id=chat_id, photo=plot_metrics)
    
    df = extract_data()
    print_metrics = text_message(df)
    plot_metrics = plot_metrics(df)
    send_metrics(print_metrics, plot_metrics)

feed_report_bot = feed_report_bot()