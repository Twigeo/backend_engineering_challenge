# ETL Data Engineering by GCP_API

## Features
### The goal of this ETL project is to:
- Extract data from a google sheet.
- Transform and aggregate the data to create new metrics.
- Visualize and store the metrics into MySQL.


The data in the google sheet is marketing reporting data for the first half-yearly 2020



### The processing application basically fetches the data and calculate two metrics:
- The network that usually has the most active users on a daily basis.
- The network that has the best “Installs” to “Subscription started” conversion rate.


## Tech
To run this app you need to create your own credentials for:
- Google sheet API key [API Quick Start](https://developers.google.com/drive/api/v3/enable-drive-api)
- MySQL database [MySQL Quick Start](https://dev.mysql.com/doc/mysql-getting-started/en/)


## Installation

- Install the dependencies in requirements.txt
- Add your own google API key in the project root
- In the source code add your google sheet link
- In the source code add your own MySQL password