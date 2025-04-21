# Social Media User Analysis with PySpark

This project involves collecting social media user data using the open-source API `randomuser.me` and analyzing it using PySpark. The data is fetched over a period of 10 minutes, with requests sent every 2 seconds. The collected data is then written to a Parquet file using the Pandas library, and later analyzed with PySpark to answer several questions regarding the users' demographics.

## Project Overview

1. **Data Collection**: 
   - A Python script collects user data from the `randomuser.me` API at 2-second intervals for 10 minutes.
   - The data is stored in a Parquet file using the Pandas library.

2. **Data Analysis**:
   - PySpark is used to answer the following questions:
     1. The average age of male and female users, as well as their average usage duration of the social media application.
     2. The average age and usage duration of male and female users by country.
     3. The top 3 oldest male and female users by country.

## Project Files

1. **`get_and_write_data.py`**: 
   - Fetches user data from the `randomuser.me` API and saves it as a Parquet file.

2. **`spark_analysis.py`**: 
   - Analyzes the collected data using PySpark and answers the questions specified in the project description.

## Setup

To run this project locally, follow these steps:

### Prerequisites

- Python 3.6 or later
- PySpark
- Pandas
- Requests

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/your-username/social-media-user-analysis.git
   cd social-media-user-analysis
