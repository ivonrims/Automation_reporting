# Automation of app reporting

### Tools:
- Python
- ClickHouse
- ETL-pipeline in Airflow
- Telegram-bot

### Description

We have an application that consists of a news feed and a messaging service. The goal of the project is to automate the visualization and sending of information about all key metrics via a telegram bot at the same time every day.

### Step 1. 

First, let's automate the basic reporting of our application. Let's set up automatic sending of an analytical summary via telegram every morning! What we need for this:

1) Create our own telegram bot using @BotFather

2) Write a script to build a news feed report. The report should consist of two parts:

- text with information about the values of key metrics (**DAU, Views, Likes, CTR**) for the previous day
- figures with metrics values for the previous 7 days

**Results:**

We receive a feed report in Telegram every day at 11:00 am.

### Step 2. 

At this step, we want to collect a report on the operation of the entire application as a whole. To achieve this, the report must contain metrics for the application as a whole, or it can display metrics for each part of the application - for the news feed and for the messenger.

**Results:**

We receive a feed report in Telegram every day at 11:00 am.
