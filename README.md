Phonepe Pulse Data Visualization and Exploration
Data Visualization and Exploration : A User-Friendly Tool Using Streamlit and Plotly.This project provides a user-friendly tool for exploring and visualizing data from the Phonepe Pulse Github repository. The solution includes data extraction, transformation, database insertion, and dashboard creation using Streamlit and Plotly in Python.
Technologies Used:
1.Plotly - (To plot and visualize the data) 
2.Pandas - (To Create a DataFrame with the scraped data) 
3.mysql.connector - (To store and retrieve the data) 
4.Streamlit - (To Create Graphical user Interface) 
5.json - (To load the json files) 

Workflow
Step 1:
Importing the Libraries: Need to import all the required modules using pip install --module name
        import git
        import subprocess
        import os
        import pandas as pd
        import json
        import streamlit as st
        import plotly.express as pt
        import plotly.graph_objects as go

Step 2:
Data Extraction:
Clone the Github using scripting to fetch the data from the Phonepe pulse Github repository and store it in a suitable format such as JSON. Use the below syntax to clone the phonepe github repository into your local drive.

Convert the Repository Data to Dataframe using Pandas Library:
       
Step 3:
Data insertion into SQL.
Create the required tables and load the Dataframe into SQL.
        import mysql.connector
        mydb = mysql.connector.connect(host="localhost",user="root", password="")
        print(mydb)
        mycursor = mydb.cursor(buffered=True)

        mycursor.execute("Create database phonepe")
        mycursor.execute("use phonepe")


Step 4:
Data Visualization using Plotly in Streamlit application
Write the required Selectbox,radio buttons to make the Streamlit app as Interactive
         st.title("Welcome to dataexplore")
            st.header("All India")
            st.image("E:\\Category_Flag maps of India - Wikimedia Commons.jpg", use_column_width=True)
    
    
            menu_options = {
                "Transaction": ["Transaction Count", "Amount"],
                "Users": ["RegisteredUsers", "AppOpens"],
            }
Create the apt map for visuals and display in Streamlit application.
Step 5:
Run the application as streamlit run phonepe.py



