# %%
import pandas as pd
from pandas.plotting import autocorrelation_plot
import matplotlib.pyplot as plt
import seaborn as sns
import boto3

# Initialize a boto3 client
s3_client = boto3.client('s3')
bucket_name = 'myweatherdata-washington-dc'
file_key = 'csv/transformed_data.csv'

# Download the file from S3
s3_client.download_file(bucket_name, file_key, 'transformed_data.csv')
df = pd.read_csv('transformed_data.csv')

# %%
# Convert 'dt' column to datetime format
df['dt'] = pd.to_datetime(df['dt'])

df_cleaned = df.drop_duplicates()

numerical_columns = df_cleaned.select_dtypes(include=['float64', 'int64']).columns
df_cleaned[numerical_columns] = df_cleaned[numerical_columns].fillna(df_cleaned[numerical_columns].mean())

# %%
# Set the aesthetic style of the plots
sns.set_style('whitegrid')

# Create a figure to hold the plots
fig, axes = plt.subplots(4, 1, figsize=(12, 16))

# Plot 1: Temperature Over Time
sns.lineplot(x='dt', y='temp', data=df_cleaned, ax=axes[0], color='red')
axes[0].set_title('Temperature Over Time')
axes[0].set_xlabel('Date')
axes[0].set_ylabel('Temperature (°F)')

# Plot 2: Pressure Over Time
sns.lineplot(x='dt', y='pressure', data=df_cleaned, ax=axes[1], color='blue')
axes[1].set_title('Pressure Over Time')
axes[1].set_xlabel('Date')
axes[1].set_ylabel('Pressure (hPa)')

# Plot 3: Humidity Over Time
sns.lineplot(x='dt', y='humidity', data=df_cleaned, ax=axes[2], color='green')
axes[2].set_title('Humidity Over Time')
axes[2].set_xlabel('Date')
axes[2].set_ylabel('Humidity (%)')

# Plot 4: Dew Point Over Time
sns.lineplot(x='dt', y='dew_point', data=df_cleaned, ax=axes[3], color='purple')
axes[3].set_title('Dew Point Over Time')
axes[3].set_xlabel('Date')
axes[3].set_ylabel('Dew Point (°F)')

# Adjust layout to prevent overlap and show the plot
plt.tight_layout()
plt.show()

# Save the figure to a file
fig.savefig('temperature_over_time.png')

# Upload the figure to S3
s3_client.upload_file('temperature_over_time.png', bucket_name, 'plots/temperature_over_time.png')

# %%
# Creating a new column for month extraction
df_cleaned['month'] = df_cleaned['dt'].dt.month

# Set up the figure layout
fig, axes = plt.subplots(4, 1, figsize=(12, 20))

# Plot 1: Rolling Average for Temperature
df_cleaned['temp_rolling_avg'] = df_cleaned['temp'].rolling(window=7, center=True).mean()
sns.lineplot(x='dt', y='temp_rolling_avg', data=df_cleaned, ax=axes[0], color='red')
axes[0].set_title('7-Day Rolling Average of Temperature')
axes[0].set_xlabel('Date')
axes[0].set_ylabel('Temperature (°F)')

# Plot 2: Heatmap of Temperature by Month
pivot_table = df_cleaned.pivot_table(values='temp', index=df_cleaned['dt'].dt.day, columns=df_cleaned['dt'].dt.month, aggfunc='mean')
sns.heatmap(pivot_table, ax=axes[1], cmap='coolwarm')
axes[1].set_title('Daily Average Temperature by Month')
axes[1].set_xlabel('Month')
axes[1].set_ylabel('Day of Month')

# Plot 3: Box Plots of Temperature by Month
sns.boxplot(x='month', y='temp', data=df_cleaned, ax=axes[2], palette='coolwarm')
axes[2].set_title('Temperature Distribution by Month')
axes[2].set_xlabel('Month')
axes[2].set_ylabel('Temperature (°F)')

# Plot 4: Autocorrelation Plot for Temperature
autocorrelation_plot(df_cleaned['temp'], ax=axes[3])
axes[3].set_title('Autocorrelation of Temperature')
axes[3].set_xlabel('Lag')
axes[3].set_ylabel('Autocorrelation')

# Adjust layout to prevent overlap and show the plot
plt.tight_layout()
plt.show()

# Save the figure to a file
fig.savefig('time_series_analysis.png')

# Upload the figure to S3
s3_client.upload_file('time_series_analysis.png', bucket_name, 'plots/time_series_analysis.png')
# %%
