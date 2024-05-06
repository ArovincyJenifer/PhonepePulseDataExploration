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

Step 2:
Data Extraction:
Clone the Github using scripting to fetch the data from the Phonepe pulse Github repository and store it in a suitable format such as JSON. Use the below syntax to clone the phonepe github repository into your local drive.
def clone_repository(repo_url, destination_folder):
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
Convert the Repository Data to Dataframe using Pandas
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
Step 3:
Data insertion into SQL.
Create the required tables and load the Dataframe into SQL.
import mysql.connector

mydb = mysql.connector.connect(host="localhost",user="root", password="")
print(mydb)
mycursor = mydb.cursor(buffered=True)

#mycursor.execute("Create database phonepe")
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



