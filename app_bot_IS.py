import numpy as np
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import io
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Parameters for the tg-bot 
# chat_id = 117705970
chat_id = -4042591545
my_token = '6384523160:AAHLGiud-LcmjE-COmJie_wcyXnxeuGw3r4'

# Parameters to access ClickHouse
connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': 'dpo_python_2020',
            'user': 'student',
            'database': 'simulator'
        }

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
def app_report_bot():
    # Task to extract data from ClickHouse for feed reports
    @task()
    def extract_data_feed(connection):
        q_feed = """ select *, 
        round(100*android_users/DAU, 2) android_users_p, round(100*iOS_users/DAU, 2) iOS_users_p,
        round(100*organic_traffic/new_users, 2) organic_traffic_p, round(100*ads_traffic/new_users, 2) ads_traffic_p
        from
        (select * from
        (select toDate(time) date, uniqExact(user_id) DAU,
        uniqExact(post_id) posts, countIf(action='view') views, 
        countIf(action='like') likes, countIf(action='like')/countIf(action='view') CTR
        from simulator_20231020.feed_actions
        where date between today() - 7 and yesterday() 
        group by date) t1

        join

        (select date, countIf(os = 'iOS') iOS_users, countIf(os = 'Android') android_users
        from
        (select toDate(time) date, user_id, max(os) os
        from simulator_20231020.feed_actions
        where toDate(time) between today() - 7 and yesterday() 
        group by toDate(time), user_id) t
        group by date) t2

        using(date)) temp

        join

        (select start_date date, uniqExact(user_id) new_users, 
        countIf(source='ads') ads_traffic, countIf(source='organic') organic_traffic
        from
        (select distinct user_id, source, min(toDate(time)) start_date
        from simulator_20231020.feed_actions
        group by user_id, source
        having start_date between today() - 7 and yesterday()) t
        group by date) t3

        using(date)

        order by date desc """

        df_feed = ph.read_clickhouse(q_feed, connection=connection)
        return df_feed
    # Task to extract data for message reports
    @task()
    def extract_data_messages(connection):
        q_messages = """ select *, 
        round(100*android_users/DAU, 2) android_users_p, round(100*iOS_users/DAU, 2) iOS_users_p
        from 
        (select toDate(time) date, uniqExact(user_id) DAU, count(user_id) messages
        from simulator_20231020.message_actions
        group by date) t1

        join 

        (select date, countIf(os = 'iOS') iOS_users, countIf(os = 'Android') android_users
        from
        (select toDate(time) date, user_id, max(os) os
        from simulator_20231020.message_actions
        where toDate(time) between today() - 7 and yesterday() 
        group by toDate(time), user_id) t
        group by date) t2 

        using(date)

        order by date desc """

        df_messages = ph.read_clickhouse(q_messages, connection=connection)
        return df_messages
    # Task to extract data for both feed & message reports
    @task()
    def extract_data_active_users(connection):
        q_active_users = """ select date, uniqExact(user_id) active_users
        from
        (select *
        from 
        (select toDate(time) date, user_id 
        from simulator_20231020.feed_actions
        where toDate(time) between today() - 7 and yesterday()) t1
        join 
        (select toDate(time) date, user_id 
        from simulator_20231020.message_actions
        where toDate(time) between today() - 7 and yesterday()) t2
        using(date, user_id))
        group by date
        order by date desc """ 

        df_active_users = ph.read_clickhouse(q_active_users, connection=connection)
        return df_active_users
    # Task to create a report message for feed report (text is in Russian) 
    @task()
    def print_metrics_feed(df_feed):
        metrics_feed = ['DAU', 'views', 'likes', 'CTR', 'new_users', 'posts']
        metrics_dict = {}
        for i in range(len(metrics_feed)):
            metrics_dict[metrics_feed[i]] = [df_feed[metrics_feed[i]].iloc[0].round(4), (100*(df_feed[metrics_feed[i]].iloc[0]/df_feed[metrics_feed[i]].iloc[1] - 1)).round(2)]

        print_metrics_feed = 'Значение метрик ленты новостей за вчерашний день ({yesterday}): \n \n\
- DAU = {DAU} ({DAU_diff}%) - из них iOS = {ios}%, Android = {android}% \n\
- Количество постов = {posts} ({posts_diff}%) \n\
- Лайки = {likes} ({likes_diff}%) \n\
- Просмотры = {views} ({views_diff}%) \n\
- CTR = {CTR} ({CTR_diff}%) \n\
- Новые пользователи = {new_users} ({new_users_diff}) \n\
- из них пришли через ads = {ads}%, organic = {organic}% \n \n\
В скобках указан относительный прирост по сравнению с предыдущим днем ({yesterday_1})'

        print_metrics_feed = print_metrics_feed.format(yesterday = df_feed.date.iloc[0].date(), 
                             DAU = metrics_dict['DAU'][0], DAU_diff = metrics_dict['DAU'][1],
                             posts = metrics_dict['posts'][0], posts_diff = metrics_dict['posts'][1],
                             likes = metrics_dict['likes'][0], likes_diff = metrics_dict['likes'][1],
                             views = metrics_dict['views'][0], views_diff = metrics_dict['views'][1],
                             CTR = metrics_dict['CTR'][0], CTR_diff = metrics_dict['CTR'][1],
                             new_users = metrics_dict['new_users'][0], new_users_diff = metrics_dict['new_users'][1],
                             yesterday_1 = df_feed.date.iloc[1].date(),
                             ios = df_feed.iOS_users_p.iloc[0],
                             android = df_feed.android_users_p.iloc[0],
                             organic = df_feed.organic_traffic_p.iloc[0],
                             ads = df_feed.ads_traffic_p.iloc[0])
        return print_metrics_feed
    # Task to create a report message for message report (text is in Russian)
    @task()
    def print_metrics_messages(df_messages):
        metrics_messages = ['DAU', 'messages']
        metrics_dict_messages = {}
        for i in range(len(metrics_messages)):
            metrics_dict_messages[metrics_messages[i]] = [df_messages[metrics_messages[i]].iloc[0].round(4), (100*(df_messages[metrics_messages[i]].iloc[0]/df_messages[metrics_messages[i]].iloc[1] - 1)).round(2)]

        print_metrics_messages = 'Значение метрик сервиса сообщений за вчерашний день ({yesterday}): \n \n\
- DAU = {DAU} ({DAU_diff}%) - из них iOS = {ios}%, Android = {android}% \n\
- Количество сообщений = {messages} ({messages_diff}%) \n \n\
В скобках указан относительный прирост по сравнению с предыдущим днем ({yesterday_1})'

        print_metrics_messages = print_metrics_messages.format(yesterday = df_messages.date.iloc[0].date(), 
                             DAU = metrics_dict_messages['DAU'][0], DAU_diff = metrics_dict_messages['DAU'][1], 
                             messages = metrics_dict_messages['messages'][0], messages_diff = metrics_dict_messages['messages'][1],
                             yesterday_1 = df_messages.date.iloc[1].date(),
                             ios = df_messages.iOS_users_p.iloc[0],
                             android = df_messages.android_users_p.iloc[0])
        return print_metrics_messages
    # Task to plot the metrics for feed report (axes are in Russian) 
    @task()
    def plot_metrics_feed(df_feed):
        fig, axs = plt.subplots(4,2, figsize=(18,18))
        fig.suptitle('Метрики ленты новостей за последние 7 дней', fontsize=18)

        axs[0,0].plot(df_feed.date, df_feed.DAU, linewidth=3)
        axs[0,0].set_title('DAU', fontsize=14)
        axs[0,0].grid(axis='both', alpha=.3)
        axs[0,0].set_xlim([df_feed.date.iloc[-1], df_feed.date.iloc[0]])

        axs[0,1].bar(df_feed.date, df_feed.android_users, .5, color='g', label='Android users')
        axs[0,1].bar(df_feed.date, df_feed.iOS_users, .5, color='gray', label='iOS users')
        y_offset = -1600
        # For each patch (basically each rectangle within the bar), add a label.
        for bar in axs[0,1].patches:
          axs[0,1].text(
              # Put the text in the middle of each bar. get_x returns the start
              # so we add half the width to get to the middle.
              bar.get_x() + bar.get_width() / 2,
              # Vertically, add the height of the bar to the start of the bar,
              # along with the offset.
              bar.get_height() + bar.get_y() + y_offset,
              # This is actual value we'll show.
              round(bar.get_height()),
              # Center the labels and style them a bit.
              ha='center',
              color='w',
              weight='bold',
              size=10
          )
        axs[0,1].set_title('Split by platform', fontsize=14)
        axs[0,1].legend(loc="lower left")

        axs[1,0].plot(df_feed.date, df_feed.new_users, linewidth=3)
        axs[1,0].set_title('New users', fontsize=14)
        axs[1,0].grid(axis='both', alpha=.3)
        axs[1,0].set_xlim([df_feed.date.iloc[-1], df_feed.date.iloc[0]])

        axs[1,1].bar(df_feed.date, df_feed.ads_traffic, .5, color='gray', label='Ads')
        axs[1,1].bar(df_feed.date, df_feed.organic_traffic, .5, color='g', label='Organic')
        y_offset = -50
        # For each patch (basically each rectangle within the bar), add a label.
        for bar in axs[3,1].patches:
          axs[1,1].text(
              # Put the text in the middle of each bar. get_x returns the start
              # so we add half the width to get to the middle.
              bar.get_x() + bar.get_width() / 2,
              # Vertically, add the height of the bar to the start of the bar,
              # along with the offset.
              bar.get_height() + bar.get_y() + y_offset,
              # This is actual value we'll show.
              round(bar.get_height()),
              # Center the labels and style them a bit.
              ha='center',
              color='w',
              weight='bold',
              size=10
          )
        axs[1,1].set_title('Split by source', fontsize=14)
        axs[1,1].legend(loc="best")

        axs[2,0].plot(df_feed.date, df_feed.views, linewidth=3)
        axs[2,0].set_title('Views', fontsize=14)
        axs[2,0].grid(axis='both', alpha=.3)
        axs[2,0].set_xlim([df_feed.date.iloc[-1], df_feed.date.iloc[0]])

        axs[2,1].plot(df_feed.date, df_feed.posts, linewidth=3)
        axs[2,1].set_title('Number of posts', fontsize=14)
        axs[2,1].grid(axis='both', alpha=.3)
        axs[2,1].set_xlim([df_feed.date.iloc[-1], df_feed.date.iloc[0]])

        axs[3,0].plot(df_feed.date, df_feed.likes, linewidth=3)
        axs[3,0].set_title('Likes', fontsize=14)
        axs[3,0].grid(axis='both', alpha=.3)
        axs[3,0].set_xlim([df_feed.date.iloc[-1], df_feed.date.iloc[0]])

        axs[3,1].plot(df_feed.date, df_feed.CTR, linewidth=3)
        axs[3,1].set_title('CTR', fontsize=14)
        axs[3,1].grid(axis='both', alpha=.3)
        axs[3,1].set_xlim([df_feed.date.iloc[-1], df_feed.date.iloc[0]])

        fig.tight_layout()

        plot_metrics_feed = io.BytesIO() # создаем файловый объект
        plt.savefig(plot_metrics_feed)
        plot_metrics_feed.seek(0) # передвигаем курсор в начало созданного нами объекта
        plot_metrics_feed.name = 'week_feed.png'
        plt.close()

        return plot_metrics_feed
    # Task to plot the metrics for feed report (axes are in Russian)
    @task()
    def plot_metrics_messages(df_messages, df_active_users):
        fig, axs = plt.subplots(2,2, figsize=(18,10))
        fig.suptitle('Метрики сервиса сообщений за последние 7 дней', fontsize=18)

        axs[0,0].plot(df_messages.date, df_messages.DAU, linewidth=3)
        axs[0,0].set_title('DAU', fontsize=14)
        axs[0,0].grid(axis='both', alpha=.3)
        axs[0,0].set_xlim([df_messages.date.iloc[-1], df_messages.date.iloc[0]])

        axs[0,1].bar(df_messages.date, df_messages.android_users, .5, color='g', label='Android users')
        axs[0,1].bar(df_messages.date, df_messages.iOS_users, .5, color='gray', label='iOS users')
        y_offset = -100
        # For each patch (basically each rectangle within the bar), add a label.
        for bar in axs[0,1].patches:
          axs[0,1].text(
              # Put the text in the middle of each bar. get_x returns the start
              # so we add half the width to get to the middle.
              bar.get_x() + bar.get_width() / 2,
              # Vertically, add the height of the bar to the start of the bar,
              # along with the offset.
              bar.get_height() + bar.get_y() + y_offset,
              # This is actual value we'll show.
              round(bar.get_height()),
              # Center the labels and style them a bit.
              ha='center',
              color='w',
              weight='bold',
              size=8
          )
        axs[0,1].set_title('Split by platform', fontsize=14)
        axs[0,1].legend(loc="best")

        axs[1,0].plot(df_active_users.date, df_active_users.active_users, linewidth=3)
        axs[1,0].set_title('Users using both feed & message services', fontsize=14)
        axs[1,0].grid(axis='both', alpha=.3)
        axs[1,0].set_xlim([df_messages.date.iloc[-1], df_messages.date.iloc[0]])

        axs[1,1].bar(df_messages.date, df_messages.messages, .5, color='gray')
        axs[1,1].set_title('Messages sent', fontsize=14)
        y_offset = - 1600
        for bar in axs[1,1].patches:
          axs[1,1].text(
              # Put the text in the middle of each bar. get_x returns the start
              # so we add half the width to get to the middle.
              bar.get_x() + bar.get_width() / 2,
              # Vertically, add the height of the bar to the start of the bar,
              # along with the offset.
              bar.get_height() + bar.get_y() + y_offset,
              # This is actual value we'll show.
              round(bar.get_height()),
              # Center the labels and style them a bit.
              ha='center',
              color='w',
              weight='bold',
              size=8
          )

        fig.tight_layout()

        plot_metrics_messages = io.BytesIO() # create a file object
        plt.savefig(plot_metrics_messages)
        plot_metrics_messages.seek(0) # to move cursor to the begining of the object we just created
        plot_metrics_messages.name = 'week_msg.png'
        plt.close()

        return plot_metrics_messages
    
    @task()
    def send_metrics(chat_id, my_token, print_metrics_feed, print_metrics_messages, plot_metrics_feed, plot_metrics_messages):
        bot = telegram.Bot(token=my_token) # access to tg-bot
        bot.sendMessage(chat_id=chat_id, text=print_metrics_feed)
        bot.sendMessage(chat_id=chat_id, text=print_metrics_messages)
        bot.sendPhoto(chat_id=chat_id, photo=plot_metrics_feed)
        bot.sendPhoto(chat_id=chat_id, photo=plot_metrics_messages)

    df_feed = extract_data_feed(connection)
    df_messages = extract_data_messages(connection)
    df_active_users = extract_data_active_users(connection)
    print_metrics_feed = print_metrics_feed(df_feed)
    print_metrics_messages = print_metrics_messages(df_messages)
    plot_metrics_feed = plot_metrics_feed(df_feed)
    plot_metrics_messages = plot_metrics_messages(df_messages, df_active_users)
    send_metrics = send_metrics(chat_id, my_token, print_metrics_feed, print_metrics_messages, plot_metrics_feed, plot_metrics_messages)
    
app_report_bot = app_report_bot()