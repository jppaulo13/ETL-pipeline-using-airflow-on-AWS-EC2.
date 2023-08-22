# ETL-pipeline-using-airflow-on-AWS-EC2.

In this project, I developed a data pipeline to extract real time weather data from the OpenWeatherMap API, with a specific focus on Lisbon's weather conditions. I refined the acquired data and stored it within an Amazon S3 repository, formatted in CSV. The entire process was orchestrated using Apache AirFlow.

Leveraging the capabilities of Apache Airflow, I structured a sequential plan to load daily information of Lisbon's weather conditions in amazon S3 repository and also incorporated error-handling mechanisms. In the event of issues, the workflow was designed to trigger email notifications. This orchestrated workflow was conducted entirely within the Amazon Web Services (AWS) cloud environment, with the primary utilization of EC2 instance for execution.

The code was developed with Python and I utilized libraries such JSON and Pandas. This project really helped me get a good grasp of how Apache Airflow works, such as by providing insights of Directed Acyclic Graphs (DAGs) and Operators.



