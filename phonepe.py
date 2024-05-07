import git
import subprocess
import os
import pandas as pd
import json
import streamlit as st
import plotly.express as pt
import plotly.graph_objects as go
from sqlalchemy import create_engine
import sqlalchemy
from streamlit import connections
import pymysql
import plotly.express as px
import geopandas as gpd
import altair as alt

# def clone_repository(repo_url, destination_folder):
#     try:
#         # Run git clone command
#         result = subprocess.run(['git', 'clone', repo_url, destination_folder], capture_output=True, text=True)
        
#         # Check if the command was successful
#         if result.returncode == 0:
#             print("Repository cloned successfully.")
#         else:
#             print("Error:", result.stderr)
#     except Exception as e:
#         print("Error:", e)

# repo_url = "https://github.com/PhonePe/pulse.git"
# destination_folder = "E:\Phonepepulsedata"
# clone_repository(repo_url, destination_folder)

#Creating Dataframe of Aggregated transaction Data:

# path = "E:\\Phonepepulsedata\\data\\aggregated\\transaction\\country\\india\\state\\"
# agg_transaction_state_list = os.listdir(path)
# #print(agg_transaction_state_list)

#1. Aggreagted Transaction

def aggregated_transaction():
    path = "E:\\Phonepepulsedata\\data\\aggregated\\transaction\\country\\india\\state\\"
    agg_transaction_state_list = os.listdir(path)
    #print(agg_transaction_state_list)
    aggregated_transaction_data = {"State":[],"Year":[],"Quater":[],"TransationType":[],"Total_number_Transactions":[],"Amount":[]}
    for i in agg_transaction_state_list:
        p_i = path+i+"\\"
        agg_transaction_year = os.listdir(p_i)
        for j in agg_transaction_year:
            p_j = p_i+j+"\\"
            agg_transaction_quarter = os.listdir(p_j)
            for k in agg_transaction_quarter:
                p_k = p_j+k
                Data = open(p_k,'r')
                D = json.load(Data)
                for z in D['data']['transactionData']:
                    name = z['name']
                    count = z['paymentInstruments'][0]['count']
                    amount = z['paymentInstruments'][0]['amount']
                    aggregated_transaction_data['State'].append(i)
                    aggregated_transaction_data['Year'].append(j)
                    aggregated_transaction_data['Quater'].append(k.strip('.json'))
                    aggregated_transaction_data['TransationType'].append(name)
                    aggregated_transaction_data['Total_number_Transactions'].append(count)
                    aggregated_transaction_data['Amount'].append(amount)
    return aggregated_transaction_data
aggregated = aggregated_transaction()
aggregated_transaction_data_df = pd.DataFrame(aggregated)
#print(aggregated_transaction_data_df)



#2.Function for aggregated user Data:
path = "E:\\Phonepepulsedata\\data\\aggregated\\user\\country\\india\\state\\"

def aggregated_user():
    path = "E:\\Phonepepulsedata\\data\\aggregated\\user\\country\\india\\state\\"
    aggregated_user_state_list = os.listdir(path)
    aggregated_user_data ={"State":[],"Year":[],"Quarter":[],"RegistedUsers":[],"AppOpens":[]}
    for i in aggregated_user_state_list:
        p_i = path+i+"\\"
        aggregated_user_year_list = os.listdir(p_i)
        for j in aggregated_user_year_list:
            p_j = p_i+j+"\\"
            aggregated_user_quarter = os.listdir(p_j)
            for k in aggregated_user_quarter:
                p_k = p_j+k
                with open(p_k, 'r') as file:
                    try:
                        data = json.load(file)
                        reg_user = data['data']['aggregated']['registeredUsers']
                        appopen = data['data']['aggregated']['appOpens']
                        aggregated_user_data['State'].append(i)
                        aggregated_user_data['Year'].append(j)
                        aggregated_user_data['Quarter'].append(k.strip('.json'))
                        aggregated_user_data['RegistedUsers'].append(reg_user)
                        aggregated_user_data['AppOpens'].append(appopen)
                            

                    except:
                        pass

    return aggregated_user_data
   

aggregated_user_phonepe = aggregated_user()
aggregated_user_df = pd.DataFrame(aggregated_user_phonepe)
#print(aggregated_user_df)


#3.Function for Map Attribute
def map_transaction():
    path = "E:\\Phonepepulsedata\\data\\map\\transaction\\hover\\country\\india\\state\\"
    map_transaction_state_list = os.listdir(path)
    map_transaction_data ={"State":[],"Year":[],"Quarter":[],"NameofState":[],"Type":[],"map_transaction_count":[],"map_transaction_amount":[]}
    for i in map_transaction_state_list:
        p_i = path+i+"\\"
        map_transaction_year_list = os.listdir(p_i)
        for j in map_transaction_year_list:
            p_j = p_i+j+"\\"
            map_transaction_quarter = os.listdir(p_j)
            for k in map_transaction_quarter:
                p_k = p_j+k
                with open(p_k, 'r') as file:
                    data = json.load(file)
                for item in data['data']['hoverDataList']:
                    name = item['name']
                    for metric in item['metric']:
                        type_ = metric['type']
                        count = metric['count']
                        amount = metric['amount']
                        map_transaction_data['State'].append(i)
                        map_transaction_data['Year'].append(j)
                        map_transaction_data['Quarter'].append(k.strip('.json'))
                        map_transaction_data['NameofState'].append(name)
                        map_transaction_data['Type'].append(type_)
                        map_transaction_data['map_transaction_count'].append(count)
                        map_transaction_data['map_transaction_amount'].append(amount)
    return map_transaction_data
 
map_transaction_phonepe = map_transaction()
map_transaction_phonepe_df = pd.DataFrame(map_transaction_phonepe)
#print(map_transaction_phonepe_df)

#4.Function for Map User data:
def map_user():
    path = "E:\\Phonepepulsedata\\data\\map\\user\\hover\\country\\india\\state\\"
    map_user_state_list = os.listdir(path)
    map_user_data ={"State":[],"Year":[],"Quarter":[],"NameofhoverState":[],"Registered_users":[],"app_opens":[]}
    for i in map_user_state_list:
        p_i = path+i+"\\"
        map_user_year_list = os.listdir(p_i)
        for j in map_user_year_list:
            p_j = p_i+j+"\\"
            map_user_quarter = os.listdir(p_j)
            for k in map_user_quarter:
                p_k = p_j+k
                with open(p_k, 'r') as file:
                    data = json.load(file)
                for state,state_data in data['data']['hoverData'].items():
                    map_user_data['NameofhoverState'].append(state)
                    map_user_data['Registered_users'].append(state_data['registeredUsers'])
                    map_user_data['app_opens'].append(state_data['appOpens'])         
                    map_user_data['State'].append(i)
                    map_user_data['Year'].append(j)
                    map_user_data['Quarter'].append(k.strip('.json'))
                               
                    
    return map_user_data
 
map_user_phonepe = map_user()
map_user_phonepe_df = pd.DataFrame(map_user_phonepe)
#print(map_user_phonepe_df)

#5.Function for Top_Transaction_district data:
def top_transaction_district():
   path = "E:\\Phonepepulsedata\\data\\top\\transaction\\country\\india\\state\\"
   
   top_transaction_data = {"State":[],"Year":[],"Quater":[],"entity_district":[],"entity_district_trans":[],"entity_district_amount":[]}  

   top_state_list = os.listdir(path)  

   for i in top_state_list:
      p_i = path+i+"\\"
      top_year_list = os.listdir(p_i)

      for j in top_year_list:
         p_j = p_i+j+"\\"
         top_quarter_list = os.listdir(p_j)

         for k in top_quarter_list:
            p_k =p_j+k
            D = open(p_k,'r')
            data = json.load(D)

            for z in data['data']['districts']:
               entity_district = z['entityName']
               entity_district_trans = z['metric']['count']
               entity_district_amount = z['metric']['amount']
               top_transaction_data['State'].append(i)
               top_transaction_data['Year'].append(j)
               top_transaction_data['Quater'].append(k.strip('.json'))
               top_transaction_data['entity_district'].append(entity_district)
               top_transaction_data['entity_district_trans'].append(entity_district_trans)
               top_transaction_data['entity_district_amount'].append(entity_district_amount)

   return top_transaction_data

a = top_transaction_district()
top_transaction_data_district_df = pd.DataFrame(a)
#print(top_transaction_data_district_df)

#6.Function for Top_Transaction_pincodes data:
def top_transaction_pincodes():
   path = "E:\\Phonepepulsedata\\data\\top\\transaction\\country\\india\\state\\"
   
   top_transaction_data = {"State":[],"Year":[],"Quater":[],"entity_pincode":[],"entity_pin_area_trans":[],"entity_pin_area_amount":[]}  

   top_state_list = os.listdir(path)  

   for i in top_state_list:
      p_i = path+i+"\\"
      top_year_list = os.listdir(p_i)

      for j in top_year_list:
         p_j = p_i+j+"\\"
         top_quarter_list = os.listdir(p_j)

         for k in top_quarter_list:
            p_k =p_j+k
            D = open(p_k,'r')
            data = json.load(D)

            for z in data['data']['pincodes']:
               entity_pincode = z['entityName']
               entity_pin_area_trans = z['metric']['count']
               entity_pin_area_amount = z['metric']['amount']
               top_transaction_data['State'].append(i)
               top_transaction_data['Year'].append(j)
               top_transaction_data['Quater'].append(k.strip('.json'))
               top_transaction_data['entity_pincode'].append(entity_pincode)
               top_transaction_data['entity_pin_area_trans'].append(entity_pin_area_trans)
               top_transaction_data['entity_pin_area_amount'].append(entity_pin_area_amount)

   return top_transaction_data


B = top_transaction_pincodes()
top_transaction_data_pin_area_df = pd.DataFrame(B)
#print(top_transaction_data_pin_area_df)

#7.Function Top_User_district Data:
def top_user_district():
   path = "E:\\Phonepepulsedata\\data\\top\\user\\country\\india\\state\\"
   
   top_user_data = {"State":[],"Year":[],"Quater":[],"entity_district":[],"registered_user_count":[]}  

   top_state_list = os.listdir(path)  

   for i in top_state_list:
      p_i = path+i+"\\"
      top_year_list = os.listdir(p_i)

      for j in top_year_list:
         p_j = p_i+j+"\\"
         top_quarter_list = os.listdir(p_j)

         for k in top_quarter_list:
            p_k =p_j+k
            D = open(p_k,'r')
            data = json.load(D)

            for z in data['data']['districts']:
               entity_district = z['name']
               registered_user_count = z['registeredUsers']
               top_user_data['State'].append(i)
               top_user_data['Year'].append(j)
               top_user_data['Quater'].append(k.strip('.json'))
               top_user_data['entity_district'].append(entity_district)
               top_user_data['registered_user_count'].append(registered_user_count)
               

   return top_user_data
c = top_user_district()
top_user_district_df = pd.DataFrame(c)
#print(top_user_district_df)

#8.Function for Top_user_pincodes_data:
def top_user_pincodes():
   path = "E:\\Phonepepulsedata\\data\\top\\user\\country\\india\\state\\"
   
   top_user_pincode_data = {"State":[],"Year":[],"Quater":[],"entity_area_pin":[],"registered_user_pincode_count":[]}  

   top_state_list = os.listdir(path)  

   for i in top_state_list:
      p_i = path+i+"\\"
      top_year_list = os.listdir(p_i)

      for j in top_year_list:
         p_j = p_i+j+"\\"
         top_quarter_list = os.listdir(p_j)

         for k in top_quarter_list:
            p_k =p_j+k
            D = open(p_k,'r')
            data = json.load(D)

            for z in data['data']['pincodes']:
               entity_area_pin = z['name']
               registered_user_count = z['registeredUsers']
               top_user_pincode_data['State'].append(i)
               top_user_pincode_data['Year'].append(j)
               top_user_pincode_data['Quater'].append(k.strip('.json'))
               top_user_pincode_data['entity_area_pin'].append(entity_area_pin)
               top_user_pincode_data['registered_user_pincode_count'].append(registered_user_count )
               

   return top_user_pincode_data

C = top_user_pincodes()
top_user_pincode_df = pd.DataFrame(C)
#print(top_user_pincode_df)


#map the states correctly inorder to plot the graph:
mappings = {
    'andaman-&-nicobar-islands':'Andaman & Nicobar',	'andhra-pradesh':'Andhra Pradesh',	'arunachal-pradesh':'Arunachal Pradesh',	'assam':'Assam',	'bihar':'Bihar',	'chandigarh':'Chandigarh',	'chhattisgarh':'Chhattisgarh',	'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu',	'delhi':'Delhi',	'goa':'Goa',	'gujarat':'Gujarat',	'haryana':'Haryana',	'himachal-pradesh':'Himachal Pradesh',	'jammu-&-kashmir':'Jammu & Kashmir',	'jharkhand':'Jharkhand',	'karnataka':'Karnataka',	'kerala':'Kerala',	'ladakh':'Ladakh',	'lakshadweep':'Lakshadweep',	'madhya-pradesh':'Madhya Pradesh',	'maharashtra':'Maharashtra',	'manipur':'Manipur',	'meghalaya':'Meghalaya',	'mizoram':'Mizoram',	'nagaland':'Nagaland',	'odisha':'Odisha',	'puducherry':'Puducherry',	'punjab':'Punjab',	'rajasthan':'Rajasthan',	'sikkim':'Sikkim',	'tamil-nadu':'Tamil Nadu',	'telangana':'Telangana',	'uttarakhand':'Uttarkhand',	'tripura':'Tripura',	'uttar-pradesh':'Uttar Pradesh','west-bengal':'West Bengal'
}

aggregated_transaction_data_df['State'] = aggregated_transaction_data_df['State'].replace(mappings)
aggregated_user_df['State'] = aggregated_user_df['State'].replace(mappings)
map_transaction_phonepe_df['State'] = map_transaction_phonepe_df['State'].replace(mappings)
map_user_phonepe_df['State'] = map_user_phonepe_df['State'].replace(mappings)
top_transaction_data_district_df['State'] = top_transaction_data_district_df['State'].replace(mappings)
top_transaction_data_pin_area_df['State'] = top_transaction_data_pin_area_df['State'].replace(mappings)
top_user_district_df['State'] = top_user_district_df['State'].replace(mappings)
top_user_pincode_df['State'] = top_user_pincode_df['State'].replace(mappings)

# Data Pre-processing:

# aggregated_transaction_data_df.info()
# aggregated_user_df.info()
# map_transaction_phonepe_df.info()
# map_user_phonepe_df.info()
# top_transaction_data_district_df.info()
# top_transaction_data_pin_area_df.info()
# top_user_district_df.info()
# top_user_pincode_df.info()

# Data Storage into SQL:
import mysql.connector

mydb = mysql.connector.connect(host="localhost",user="root", password="")
print(mydb)
mycursor = mydb.cursor(buffered=True)

#mycursor.execute("Create database phonepe")
mycursor.execute("use phonepe")

engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw="",
                               db="phonepe"))

# 1. aggregated_transaction_data_df
# mycursor.execute("Create table phonepe.agg_trans (State Varchar(20),year year,Quarter INT,TransationType Varchar(20),Total_number_Transaction INT,Amount floar)") 
# rows_to_insert = [tuple(row) for row in aggregated_transaction_data_df.values]
# sql = '''INSERT INTO phonepe.agg_trans(State,year,Quarter,TransationType,Total_number_Transaction,Amount) values (%s, %s, %s,%s,%s,%s)'''
# mycursor.executemany(sql,rows_to_insert)
# mydb.commit()

# 2. aggregated_user_df

# mycursor.execute("Create table phonepe.agg_user (State Varchar(20),year year,Quarter INT,Registeredusers INT,AppOpens INT,BrandName Varchar(20),RegisteredUsers_brand INT,	Percentage float)") 
# rows_to_insert = [tuple(row) for row in aggregated_user_df.values]
# sql = '''INSERT INTO phonepe.agg_user(State,year,Quarter,Registeredusers,AppOpens,BrandName,RegisteredUsers_brand,Percentage) values (%s, %s, %s,%s,%s,%s,%s,%s)'''
# mycursor.executemany(sql,rows_to_insert)
# mydb.commit()

# 3. map_transaction_phonepe_df

# mycursor.execute("Create table phonepe.map_trans (State Varchar(20),year year,Quarter INT,District Varchar(20),Type varchar(20),Map_Trans_Count INT,Map_Trans_Amount Float)") 
# rows_to_insert = [tuple(row) for row in map_transaction_phonepe_df.values]
# sql = '''INSERT INTO map_trans (State,year,Quarter,District,Type,Map_Trans_Count,Map_Trans_Amount) values (%s,%s,%s,%s,%s,%s,%s)'''
#mycursor.executemany(sql,rows_to_insert)
# mydb.commit()

# 4. map_user_phonepe_df

# mycursor.execute("Create table phonepe.map_user (State Varchar(20),year year,Quarter,INT,hoverdistrict Varchar(20),registered_users INT,appopen INT)")

# rows_to_insert = [tuple(row) for row in map_user_phonepe_df.values]
# sql = '''INSERT INTO phonepe.map_user (State,year,Quarter,hoverdistrict,registered_users,appopen) values (%s,%s,%s,%s,%s,%s)'''
#mycursor.executemany(sql,rows_to_insert)
# mydb.commit()

#5. top_transaction_data_district_df

# mycursor.execute("Create table phonepe.top_trans_dist (State varchar(20),year year,Quarter INT,entity_district varchar(20),entity_district_trans int,entity_district_amount Float)")
# rows_to_insert = [tuple(row) for row in top_transaction_data_district_df.values]
# #print(rows_to_insert)
# sql = '''INSERT INTO phonepe.top_trans (State,year,Quarter,entity_district,entity_district_trans,entity_district_amount) values (%s,%s,%s,%s,%s,%s))'''
# mycursor.executemany(sql,rows_to_insert)
# mydb.commit()
# out=mycursor.fetchall()
# #from tabulate import tabulate
# print(tabulate(out,headers=[i[0] for i in mycursor.description],  tablefmt='psql'))

#6. top_transaction_data_pin_area_df
# mycursor.execute("Create table phonepe.top_trans_pincode (State varchar(20),Year year,Quarter INT,entity_pincode INT,entity_pin_area_trans INT,entity_pin_area_amount float)")
# rows_to_insert = [tuple(row) for row in top_transaction_data_pin_area_df.values]
# sql = '''INSERT INTO phonepe.top_trans_pincode (State,Year,Quarter,entity_pincode,entity_pin_area_trans,entity_pin_area_amount) values (%s,%s,%s,%s,%s,%s)'''
# mycursor.executemany(sql,rows_to_insert)
# mydb.commit()

#7.top_user_district_df
# mycursor.execute("Create table phonepe.top_user_district (State varchar(20),Year INT,Quarter INT,entity_district Varchar(20),reg_user_count INT)")
# mydb.commit()
# rows_to_insert = [tuple(row) for row in top_user_district_df.values]
# sql = '''INSERT INTO phonepe.top_user_district (State,Year,Quarter,entity_district,reg_user_count) values (%s,%s,%s,%s,%s)'''
# mycursor.executemany(sql,rows_to_insert)
# mydb.commit()

#8.top_user_pincode_df

# mycursor.execute("Create table phonepe.top_user_pincde (State varchar(20),Year INT,Quarter INT,entity_area_pincode INT,reg_user_pin_count INT)")
# mydb.commit()
# rows_to_insert = [tuple(row) for row in top_user_pincode_df.values]
# sql = '''INSERT INTO phonepe.top_user_pincde(State,Year,Quarter,entity_area_pincode,reg_user_pin_count) values (%s,%s,%s,%s,%s)'''
# mycursor.executemany(sql,rows_to_insert)
# mydb.commit()


#Streamlit Design:

css = """
<style>
body {
    background-color: #f0f2f6; /* Set background color */
    }
    .sidebar .sidebar-content {
        background-color: #333333; /* Set sidebar background color */
        color: white; /* Set sidebar text color */
    }
    <p style='color: blue; font-size: 18px; font-style: italic;'>
    {
    
    }
    </style>
    """
st.set_page_config(page_title = "Phonepe Pulse Data Exploration")
st.markdown(css,unsafe_allow_html=True)

def home_page():
    st.title("Welcome to PhonePe Pulse Data Exploration")
    #st.write("Welcome to the Home page!")
    st.markdown(css,unsafe_allow_html=True)
    image_url = "E:\\phonepe.jpg"
    #st.image("E:\\phonepe.jpg",width=100)
    col1, col2 = st.columns([1, 4])
    with col1:
        st.image(image_url, width=150)
    with col2:
        st.write("PhonePe is an Indian digital payments and financial services company headquartered in Bengaluru, Karnataka, India. PhonePe was founded in December 2015, by Sameer Nigam, Rahul Chari and Burzin Engineer. The PhonePe app, based on the Unified Payments Interface (UPI), went live in August 2016.The PhonePe app is accessible in 11 Indian languages. It enables users to perform various financial transactions such as sending and receiving money, recharging mobile and DTH, making utility payments, conducting in-store payments..")
        st.write("The Indian digital payments story has truly captured the world's imagination. From the largest towns to the remotest villages, there is a payments revolution being driven by the penetration of mobile phones, mobile internet and state-of-the-art payments infrastructure built as Public Goods championed by the central bank and the government. Founded in December 2015, PhonePe has been a strong beneficiary of the API driven digitisation of payments in India. When we started, we were constantly looking for granular and definitive data sources on digital payments in India. PhonePe Pulse is our way of giving back to the digital payments ecosystem.")
    
def phonepe_data_analysis_page():
    st.title("Welcome to PhonePe Data Analysis")
    selected_option = st.sidebar.selectbox(
    'Choose any one of the SQL Query',
    ('---Select a Query----',
        '1.List out the top 5 Districts across India in Phonepe Transaction','2. List the 2018 Q1 total payment and 2023 Q4 payment done',
        '3. List the top 5 States in India who has done Phonepe transaction in last year',
        '4.Mention the States with low in Transaction Count and Users in 2023','5. List the top 5 States in India who has more phonepe Users.',
        '6.How is the Contribution of Tier2 Cities in transaction and users perspective',
        '7.List out the districts in Tamilnadu who is using Phonepe',
        '8.List out the performance of Union Territories as part of Categorical Data',
        '9. List out top 10 pincodes in Usercount',
        '10.List out the Year had Zero AppOpens'

        ))
# Display the selected option

    if selected_option == '1.List out the top 5 Districts across India in Phonepe Transaction':

        st.write('You selected:', selected_option)
        query_df = pd.read_sql("""                               
                                Select State,entity_district as Districts,entity_district_trans as Transaction_Count from top_trans_dist group by State order by entity_district_trans desc LIMIT 5;""",engine)
        query_df.index = query_df.index + 1
        st.write(query_df)

        fig6 = px.bar(query_df, x='Districts', y='Transaction_Count', color='State', barmode='group',title=f'Top 5 Districts')
        fig6.update_xaxes(tickangle=45, tickfont=dict(size=20))
        st.plotly_chart(fig6)
        st.subheader("Insight")
        stream_data = "It is evident that all Tier 1 cities in India are  utilizing digital payments more.This trend is driven by the presence of IT hubs, industries, and high population densities in these cities.As a result, the number of digital payment users is higher in these areas"
        st.write(stream_data)
        
    elif selected_option == '2. List the 2018 Q1 total payment and 2023 Q4 payment done':

        st.write('You selected:', selected_option)
        query_df = pd.read_sql("""                               
                                SELECT year,TransationType as TransactionType, amount FROM agg_trans WHERE year in ('2018','2023') group by year,TransationType;""",engine)
        query_df.index = query_df.index + 1
        st.write(query_df)
        fig7 = px.bar(query_df, x='year', y='amount', color='TransactionType', barmode='group',labels={'amount': 'Amount', 'year': 'Year', 'TransactionType': 'Transaction Type'})
        fig7.update_layout(title='Transaction Amount by Year and Transaction Type',xaxis_title='Year',yaxis_title='Amount')
        st.plotly_chart(fig7)
        stream_data = "It's evident that India is increasingly adopting digital payment services across various domains such as merchant payments, recharges, bill payments, and peer-to-peer transactions. There has been a remarkable 500% growth in transactions from 2018 to 2023."
        st.subheader("Insight")
        st.write(stream_data)
    elif selected_option == '3. List the top 5 States in India who has done Phonepe transaction in last year':

        st.write('You selected:', selected_option)
        query_df = pd.read_sql("""                               
                             SELECT State, year,map_transaction_count as TransactionCount FROM map_trans WHERE year in ('2023') GROUP BY State, year ORDER BY TransactionCount DESC limit 5;""",engine)
        query_df.index = query_df.index + 1
        st.write(query_df)

    elif selected_option =='4.Mention the States with low in Transaction Count and Users in 2023':
        st.write('You selected:', selected_option)
        query_df = pd.read_sql("""                               
                             Select ag.State,ag.map_transaction_count,au.Registered_users from map_trans ag JOIN map_user au on ag.State = au.State where ag.year in ('2023') group by ag.State order by ag.map_transaction_count asc,au.Registered_users asc limit 10;""",engine)
        query_df.index = query_df.index + 1
        st.write(query_df)
        fig8 = px.bar(query_df, x='State', y='map_transaction_count', color='Registered_users', barmode='stack')
        st.plotly_chart(fig8)
        st.subheader("Insights")
        stream_data = "Digital transactions in Union Pradesh and the North Eastern regions are still emerging, with a significant portion of the population yet to fully embrace them. Additionally, users of the PhonePe application in these areas remain comparatively low. Efforts to promote digital literacy and enhance infrastructure could help accelerate adoption rates and foster financial inclusion in these regions."
        st.write(stream_data)


    elif selected_option == '5. List the top 5 States in India who has more phonepe Users.':
        st.write('You selected:', selected_option)
        query_df = pd.read_sql("""                               
                             SELECT state,NameofhoverState as District,Registered_users as UserCount FROM map_user GROUP BY State,District order by UserCount Desc limit 5;;""",engine)
        query_df.index = query_df.index + 1
        st.write(query_df)
        fig9 = px.pie(query_df,names='state', values='UserCount')
        st.plotly_chart(fig9)
    elif selected_option =='6.How is the Contribution of Tier2 Cities in transaction and users perspective':
        st.write('You selected:', selected_option)
        query_df = pd.read_sql("""                               
                             SELECT State,entity_district as District,entity_district_trans as Transaction from top_trans_dist where entity_district not in ('Bangalore','Delhi','Chennai','Hyderabad','Mumbai','Pune','Kolkata','Ahmedabad') group by state,District order by TRANSACTION desc limit 15;""",engine)
        query_df.index = query_df.index + 1
        st.write(query_df)
        stream_data = "Tier 2 cities are also demonstrating notable performance in PhonePe transactions.Above data highlights the strong performance of tier 2 cities in PhonePe transactions, as evidenced by the top 15 districts outside major metropolitan areas."
        st.subheader("Insight")
        st.write(stream_data)
    elif selected_option == '7.List out the top districts in Tamilnadu who is using Phonepe':
        st.write('You selected:', selected_option)
        query_df = pd.read_sql("""SELECT state,entity_district as District,reg_user_count as UserCount FROM `top_user_district` WHERE state = 'Tamil Nadu' group by District;""",engine)
        query_df.index = query_df.index + 1
        st.write(query_df)
        st.subheader("Insights")
        stream_data = "Like many other high GDP states, Tamil Nadu also boasts a significant user base across all its districts, with transactions steadily increasing over the past few years. With its robust infrastructure, sustained economic growth, high GDP, and dense population, Tamil Nadu ranks among the top 10 states in PhonePe data "
        st.write(stream_data)
    elif selected_option == '8.List out the performance of Union Territories as part of Categorical Data':
        st.write('You selected:', selected_option)
        query_df = pd.read_sql("""SELECT state,TransationType,Total_number_Transaction,Amount FROM agg_trans WHERE state in ("Ladakh","Jammu & Kashmir","Puducherry","Lakshadweep","Delhi","Chandigarh","Dadra and Nagar Have","Andaman and Nicobar Islands") group by State order by amount desc;""",engine)
        query_df.index = query_df.index + 1
        st.write(query_df)
        st.subheader("Insights")
        st.write("Delhi leads in the total number of transactions, with a significant amount of ₹1.32 billion processed through Recharge & Bill Payments. Meanwhile, Jammu & Kashmir and Chandigarh follow with relatively lower transaction volumes but still significant amounts. It's notable that while Ladakh has a lower number of transactions, the average transaction amount is comparatively higher, suggesting potential opportunities for further analysis or targeted marketing efforts in these regions")
    elif selected_option == '9. List out top 10 pincodes in Usercount':
        st.write('You selected:', selected_option)
        query_df= pd.read_sql("""SELECT state,entity_area_pincode as PINCODE,reg_user_pin_count as Userzcount FROM top_user_pincde group by state order by reg_user_pin_count desc limit 10;""",engine)
        query_df.index = query_df.index + 1
        st.write(query_df)
    elif selected_option == '10.List out the Year had Zero AppOpens':
        st.write("You Selected",selected_option)
        query_df = pd.read_sql("""SELECT DISTINCT(Year),AppOpens from agg_user where AppOpens = 0 GROUP by State,year;""",engine)
        query_df.index = query_df.index + 1
        st.write(query_df)
        st.subheader("Insights")
        st.write("After the COVID-19 pandemic, there has been a consistent trend of app opens since 2020, indicating that users have registered and begun using digital payment services.")

    
def dashboard_page():
    st.header("Welcome to Dashboard")
    #st.write("Welcome to the Dashboard page!")
    
    selected_trans_user = st.selectbox("Select Type",["Transaction","Users"])
    years = list(range(2018,2024))
    selected_year = st.selectbox("select year",years)
    quarters = ["Q1 (Jan - Mar)", "Q2 (Apr - Jun)", "Q3 (Jul - Sep)", "Q4 (Oct - Dec)"]
    selected_quarter = st.selectbox("Select Quarter", quarters)

    if(selected_trans_user == 'Transaction'):

    ##Transaction based results

        query1_df = pd.read_sql(f"""
                               SELECT sum(amount) as Totalpaymentvalue_Cr , sum(Total_number_Transaction) as AllPhonePetransactions from agg_trans WHERE year = '{selected_year}'and quarter = {quarters.index(selected_quarter)+1};""",engine)


        query1_df.index = query1_df.index+1
    
        st.write(query1_df['Totalpaymentvalue_Cr'])
        #st.write(query1_df['Totalpaymentvalue_Cr'].style.highlight_max(axis=0))
        #top 10 states
        top_states_df = pd.read_sql(f"""
                                SELECT State,sum(amount) as Totalpaymentvalue_Cr from agg_trans WHERE year = '{selected_year}'and quarter = {quarters.index(selected_quarter)+1} group by State order by Totalpaymentvalue_Cr desc limit 10;""",engine)

        top_states_df.index = top_states_df.index+1
        fig1 = px.pie(top_states_df, values='Totalpaymentvalue_Cr', names='State', title=f'{selected_trans_user} for {selected_year} - {selected_quarter}-Top 10 States Phonepe Payment')

        fig1.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig1)
    

    #top 10 district
        top_dist_df = pd.read_sql(f"""select entity_district as Districts, sum(entity_district_trans) as Transaction from top_trans_dist where year = '{selected_year}' and Quater = {quarters.index(selected_quarter)+1} group by state,entity_district order by Transaction DESC limit 10;""",engine)

        top_dist_df['Districts'] = top_dist_df['Districts'].str.upper()
        top_dist_df['Transaction'] = top_dist_df['Transaction'].apply(lambda x: "{:,.2f}".format(x))
        top_dist_df.index= top_dist_df.index+1

   

        fig2 = px.bar(top_dist_df, x='Districts', y='Transaction', title=f'{selected_trans_user} for {selected_year} - {selected_quarter}-Top 10 Districts Phonepe Transactions')

        st.plotly_chart(fig2)


    #top 10 postal
        top_pin_df = pd.read_sql(f"""select entity_pincode as Pincodes, sum(entity_pin_area_trans) as Transaction from top_trans_pincode where year = '{selected_year}' and Quarter = {quarters.index(selected_quarter)+1} group by state,entity_pincode order by Transaction DESC limit 10;""",engine)

        top_pin_df.index = top_pin_df.index+1
        fig3 = px.pie(top_pin_df, values='Transaction', names='Pincodes', title=f'{selected_trans_user} for {selected_year} - {selected_quarter}-Top 10 postal codes Phonepe Transactions')

        fig3.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig3)

    # Create a sunburst chart for Categories:
        category_df = pd.read_sql(f"""
                               SELECT State,TransationType as Categories , sum(Total_number_Transaction) as AllPhonePetransactions,SUM(Amount) AS Amount from agg_trans WHERE year = '{selected_year}'and quarter = {quarters.index(selected_quarter)+1} group by TransationType,State;""",engine)
    #st.write(category_df)
        fig4 = px.sunburst(category_df, path=['State','Categories'], values='Amount', title=f'Categorical Representation for {selected_year} - {selected_quarter}')

        st.plotly_chart(fig4)
## End of Transaction Based results
    else:
# User Based Results and Charts:

    #State Vs reistered Users

        reg_user_df = pd.read_sql(f"""SELECT DISTINCT(State),Year,RegistedUsers FROM agg_user where year = '{selected_year}' and Quarter = {quarters.index(selected_quarter)+1} GROUP by State;""",engine)

        fig5 = px.bar(reg_user_df, y='State',x= 'RegistedUsers',color='State',orientation='h',title=f'{selected_trans_user} for {selected_year} - {selected_quarter}')
        fig5.update_xaxes(tickangle=45)
        fig5.update_layout(title="State Vs Registered Users")
        st.plotly_chart(fig5)
        
        #top districts vs Users

        
        top_user_dist_df = pd.read_sql(f"""SELECT DISTINCT(State),Year,entity_district as Districts,reg_user_count as UsersCount FROM top_user_district where year = '{selected_year}' and Quarter = {quarters.index(selected_quarter)+1} GROUP by State order by UsersCount desc;""",engine)

        fig6 = px.sunburst(top_user_dist_df, path=['Districts','State'], values='UsersCount')
        fig6.update_layout(title="Hierarchical Structure of Districts Vs UsersCount")
        st.plotly_chart(fig6)

def data_explore():
    st.title("Welcome to dataexplore")
    st.header("All India")
    st.image("E:\\Category_Flag maps of India - Wikimedia Commons.jpg", use_column_width=True)
    
    
    menu_options = {
        "Transaction": ["Transaction Count", "Amount"],
        "Users": ["RegisteredUsers", "AppOpens"],
    }

    # Display the main menu as a selectbox
    main_menu = st.selectbox("Select a category", list(menu_options.keys()))

    # Based on the selection, display sub-menu options using selectbox
    if main_menu:
        sub_menu = st.selectbox("Select items", menu_options[main_menu])

        if sub_menu:
            # Load and display the map based on the sub-menu selection
            if main_menu == 'Transaction':
                if sub_menu == 'Transaction Count':
                    fig = px.choropleth(map_transaction_phonepe_df,
                                        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                                        featureidkey='properties.ST_NM',
                                        locations='State',
                                        color='map_transaction_count',
                                        color_continuous_scale='Reds',
                                        scope="asia")
                else:  # sub_menu == 'Amount'
                    fig = px.choropleth(map_transaction_phonepe_df,
                                        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                                        featureidkey='properties.ST_NM',
                                        locations='State',
                                        color='map_transaction_amount',
                                        color_continuous_scale='Reds',
                                        scope="asia")
            else:  # main_menu == 'Users'
                if sub_menu == 'RegisteredUsers':
                    fig = px.choropleth(map_user_phonepe_df,
                                        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                                        featureidkey='properties.ST_NM',
                                        locations='State',
                                        color='Registered_users',
                                        color_continuous_scale='Reds',
                                        scope="asia")
                else:  # sub_menu == 'AppOpens'
                    fig = px.choropleth(map_user_phonepe_df,
                                        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                                        featureidkey='properties.ST_NM',
                                        locations='State',
                                        color='app_opens',
                                        color_continuous_scale='Reds',
                                        scope="asia")
                
            fig.update_geos(fitbounds="locations", visible=False)
            fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
            st.plotly_chart(fig)
                        
sidebar_options = {
            "Home" : home_page,
            "Dashboard" : dashboard_page,
            "Data Explore" : data_explore,
            "Phonepe Data Analysis" : phonepe_data_analysis_page,
            
            

}

st.sidebar.title("Home")
selected_page = st.sidebar.radio("Go to",list(sidebar_options.keys()))

sidebar_options[selected_page]()








