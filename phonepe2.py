import mysql.connector
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
from PIL import Image
import plotly.express as px
# connecting to sql server
def connect_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="youtube_data"
    )
mydb = connect_db()
cursor = mydb.cursor() 
#creating database
cursor.execute('CREATE DATABASE IF NOT EXISTS Phonepe')
cursor.execute('USE Phonepe')
#converting into data frames
cursor.execute("SELECT * FROM Aggregated_transaction")
table1 = cursor.fetchall()
Aggregated_trans = pd.DataFrame(table1, columns=["State", "Year", "Quarter", "Transaction_type", "Transaction_count", "Transaction_amount"])
cursor.execute("SELECT * FROM Aggregated_user")
table2 = cursor.fetchall()
Aggre_user = pd.DataFrame(table2,columns=("State","Year","Quarter","Brand","User_count","Percentage"))
#map_trans_df
cursor = mydb.cursor()
cursor.execute("SELECT * FROM Map_transaction")
table3 = cursor.fetchall()
map_trans = pd.DataFrame(table3,columns=("State","Year","Quarter","District","Transaction_count","Transaction_amount"))
#map_user_df
cursor = mydb.cursor()
cursor.execute("SELECT * FROM Map_user")
table4 = cursor.fetchall()
map_user = pd.DataFrame(table4,columns=("State","Year","Quarter","District","Registered_Users","AppOpens"))
#top_trans_df
cursor = mydb.cursor()
cursor.execute("SELECT * FROM top_transaction")
table5 = cursor.fetchall()
top_trans = pd.DataFrame(table5,columns=("State","Year","Quarter","Pincode","Transaction_count","Transaction_amount"))
#top_user_df
cursor = mydb.cursor()
cursor.execute("SELECT * FROM Top_user")
table6 = cursor.fetchall()
top_user = pd.DataFrame(table6,columns=("State","Year","Quarter","Pincode","Registered_Users"))
cursor.close()
mydb.close()
def Transaction_amount_count_Y(df, year_selected):
    Trans_Amt_Count = df[(df["Year"] == year_selected)]
    Trans_Amt_Count.reset_index(drop=True, inplace=True)
    Trans_Amt_Count_grp = Trans_Amt_Count.groupby(["State"])[["Transaction_count", "Transaction_amount"]].sum()
    Trans_Amt_Count_grp.reset_index(inplace=True)
    df1=Trans_Amt_Count_grp
    return df1
def agg_user_amount_count_Y(df, year_selected):
    user_Amt_Count=df[(df["Year"]==year_selected)]
    user_Amt_Count.reset_index(drop=True,inplace=True)
    user_Amt_Count_grp=user_Amt_Count.groupby("State",as_index=False)["User_count"].sum()
    df2=user_Amt_Count_grp
    return df2
def map_user_amount_count_Y(df, year_selected):
    user_Amt_Count=df[(df["Year"]==year_selected)]
    user_Amt_Count.reset_index(drop=True,inplace=True)
    user_Amt_Count_grp=user_Amt_Count.groupby("State")[["Registered_Users","AppOpens"]].sum()
    user_Amt_Count_grp.reset_index(inplace=True)
    df3=user_Amt_Count_grp
    return df3
def top_user_amount_count_Y(df, year_selected):
    user_Amt_Count=df[(df["Year"]==year_selected)]
    user_Amt_Count.reset_index(drop=True,inplace=True)
    user_Amt_Count_grp=user_Amt_Count.groupby("State",as_index=False)["Registered_Users"].sum()
    df4=user_Amt_Count_grp
    return df4
def calculate_totals(data):
    total_count=data['Transaction_count'].sum()
    total_amount=data['Transaction_amount'].sum()
    return total_count,total_amount
def generate_choropleth(data,title):
    """
    Generate a choropleth map using Plotly.
    """
    fig = px.choropleth(
        data,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations='State',
        color='Transaction_amount',
        color_continuous_scale='Viridis',
        hover_data={'Transaction_amount': True, 'Transaction_count': True, 'State': True}
    )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(
        title={
            'text': title,
            'x': 0.3,  
            'xanchor': 'center',
            'yanchor': 'top'
        })
    return st.plotly_chart(fig)
#predefined queries
def query1():
    Total_Trans_amt_agg=Aggregated_trans[['State','Transaction_amount']]
    Total_Trans_amt_agg_grp=Total_Trans_amt_agg.groupby('State')['Transaction_amount'].sum().sort_values(ascending= False)
    df_1=pd.DataFrame(Total_Trans_amt_agg_grp).reset_index().head(10)
    return df_1
def query2():
    Total_Trans_amt_agg=Aggregated_trans[['State','Transaction_amount']]
    Total_Trans_amt_agg_grp=Total_Trans_amt_agg.groupby('State')['Transaction_amount'].sum().sort_values(ascending= True)
    df_2=pd.DataFrame(Total_Trans_amt_agg_grp).reset_index().head(10)
    return df_2
def query3():
     Total_user_count=Aggre_user[['Brand','User_count']]
     Total_user_count_grp=Total_user_count.groupby('Brand')['User_count'].sum().sort_values(ascending= False)
     df_3=pd.DataFrame(Total_user_count_grp).head(10).reset_index()
     return df_3
def query4():
     Total_trans_amt_map=map_trans[['District','Transaction_amount']]
     Total_trans_amt_map_grp=Total_trans_amt_map.groupby('District')['Transaction_amount'].sum().sort_values(ascending=False)
     df_4=pd.DataFrame(Total_trans_amt_map_grp).head(10).reset_index()
     return df_4
def query5():
    total_appopens=map_user[['State','AppOpens']]
    total_appopens_grp=total_appopens.groupby('State')['AppOpens'].sum().sort_values(ascending=False)
    df_5=pd.DataFrame(total_appopens_grp).head(10).reset_index()
    return df_5
def query6():
    total_appopens=map_user[['State','AppOpens']]
    total_appopens_grp=total_appopens.groupby('State')['AppOpens'].sum().sort_values(ascending=True)
    df_6=pd.DataFrame(total_appopens_grp).head(10).reset_index()
    return df_6
def query7():
    total_trans_amt_top=top_trans[['Pincode','Transaction_amount']]
    total_trans_amt_top_grp=total_trans_amt_top.groupby('Pincode')['Transaction_amount'].sum().sort_values(ascending=False)
    df_7=pd.DataFrame(total_trans_amt_top_grp).head(10).reset_index()
    return df_7
def query8():
    total_trans_amt_top=top_trans[['Pincode','Transaction_amount']]
    total_trans_amt_top_grp=total_trans_amt_top.groupby('Pincode')['Transaction_amount'].sum().sort_values(ascending=True)
    df_8=pd.DataFrame(total_trans_amt_top_grp).head(10).reset_index()
    return df_8
def query9():
    total_users_top=top_user[['Pincode','Registered_Users']]
    total_users_top_grp=total_users_top.groupby('Pincode')['Registered_Users'].sum().sort_values(ascending=False)
    df_9=pd.DataFrame(total_users_top_grp).head(10).reset_index()
    return df_9
def query10():
    total_users_top=top_user[['Pincode','Registered_Users']]
    total_users_top['Pincode']=total_users_top["Pincode"].astype(str)
    total_users_top_grp=total_users_top.groupby('Pincode')['Registered_Users'].sum().sort_values(ascending=True)
    df_10=pd.DataFrame(total_users_top_grp).head(10).reset_index()
    return df_10
#streamlit part
st.set_page_config(layout="wide")
with st.sidebar:
    selected = option_menu(
        menu_title="Main Menu",  # Title of the sidebar
        options=["Home", "Explore Data", "Data Insights"],  # Menu options
        menu_icon="menu-button",  # Main menu icon
        default_index=0,  # Default selected index
        styles={
            "nav-link": {"font-size": "16px", "text-align": "left", "margin": "-2px", "--hover-color": "#6F36AD"},
            "nav-link-selected": {"background-color": "#6F36AD", "color": "white"},
        }
    )

if selected == "Home":
    col1,col4=st.columns([3,1])
    with col1:
        st.title("📊 Phonepe Pulse Data Visualization and Exploration")
        st.subheader("Explore Digital Payment Trends Across India")
        image_path=r"C:\Users\M. TEJASWINI\Desktop\project_2\image.jpg"
        image_path1=r"C:\Users\M. TEJASWINI\Desktop\project_2\image1.jpg"
        st.markdown("""<p style='font-size: 20px;'>
        Welcome to the **PhonePe Pulse Dashboard**<br>
         This platform allows you to:<br>
        - Analyze transaction trends across different states, districts, and pincodes.<br>
        - Explore the growth of registered users in India.<br>
        - Visualize time-based trends in digital payments.
        </p>""", unsafe_allow_html=True)
        st.subheader("Types of Transactions:")
        st.markdown("""<p style='font-size: 20px;'>
                    -Recharge & bill payments<br>
                    -Peer-to-peer payments<br>
                    -Merchant payments<br>
                    -Financial Services<br>
                     </p>""", unsafe_allow_html=True
                    )
        st.subheader(" Key Features")
        st.write("""<p style='font-size: 20px;'>
        - **Interactive Visualizations**: Bar charts, pie charts.<br>
        - **Regional Insights**: Top states, districts, and pincodes.</p>""", unsafe_allow_html=True)
        st.markdown("---")
        st.markdown("<p style='font-size: 20px;'>👨‍💻 Created by Meesa Tejaswini </p>", unsafe_allow_html=True)
    with col4:
        image=Image.open(image_path)
        resized_image = image.resize((300, 300))
        st.image(resized_image)
        image1=Image.open(image_path1)
        resized_image1 = image1.resize((300, 300))
        st.image(resized_image1)
elif selected == "Explore Data":
        selected_section=st.radio("Navigate to",options=["Aggregated data","Map data","Top data"])
        if selected_section=="Aggregated data":
            selected=st.selectbox("select",["Transaction","User"])
            if selected=="Transaction":
               col1,col2=st.columns([3,1])
               with col1:
                    years= st.slider("Select The Year",Aggregated_trans["Year"].min(), Aggregated_trans["Year"].max(),Aggregated_trans["Year"].min())   
                    generate_choropleth(Transaction_amount_count_Y(Aggregated_trans, years),"Aggregated Transaction data")
               with col2:
                st.markdown(
                            """
                            <style>
                            div[data-testid="stMetricValue"] {
                                font-size: 25px; /* Change the font size */
                                /*color: #000000 ; */ /* Optional: Change font color */
                            }
                            </style>
                            """,
                            unsafe_allow_html=True,
                        )
                total_count,total_amount=calculate_totals(Aggregated_trans)
                st.metric("Total Aggregate Transaction Count",total_count)
                st.metric("Total Aggregate Transaction Amount",total_amount)
                
            elif selected=="User":
                col1,col2=st.columns([3,1])
                with col1:
                    years= st.slider("Select The Year",Aggre_user["Year"].min(), Aggre_user["Year"].max(),Aggre_user["Year"].min())
                    fig1 = px.choropleth(
                        agg_user_amount_count_Y(Aggre_user, years),
                        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                        featureidkey='properties.ST_NM',
                        locations='State',
                        color='User_count',
                        color_continuous_scale='Viridis',
                        hover_data={
                            'User_count': True,  
                            'State': True              
                        }  
                    )
                    fig1.update_layout(
                                title={
                                    "text": "Aggregated user data",
                                    "x": 0.3,  # Center the title
                                    "xanchor": "center",
                                    "yanchor": "top"
                                },)
                    fig1.update_geos(fitbounds="locations", visible=False)
                    st.plotly_chart(fig1, use_container_width=False)
                with col2:
                    st.markdown(
                            """
                            <style>
                            div[data-testid="stMetricValue"] {
                                font-size: 25px; /* Change the font size */
                                /*color: #000000 ; */ /* Optional: Change font color */
                            }
                            </style>
                            """,
                            unsafe_allow_html=True,
                        )
                    Total_user_count_agg=Aggre_user['User_count'].sum()
                    st.metric(label="Total Aggregate User Count", value=Total_user_count_agg) 
        elif selected_section=="Map data":
            selected=st.selectbox("select",["Transaction","User"])
            if selected=="Transaction":
                col1,col2=st.columns([3,1])
                with col1:
                    years= st.slider("Select The Year",map_trans["Year"].min(), map_trans["Year"].max(),map_trans["Year"].min())   
                    generate_choropleth(Transaction_amount_count_Y(map_trans, years),"Map Transaction data")
                with col2:
                    st.markdown(
                            """
                            <style>
                            div[data-testid="stMetricValue"] {
                                font-size: 25px; /* Change the font size */
                                /*color: #000000 ; */ /* Optional: Change font color */
                            }
                            </style>
                            """,
                            unsafe_allow_html=True,
                        )
                    total_count,total_amount=calculate_totals(map_trans)
                    st.metric("Total Map Transaction Count",total_count)
                    st.metric("Total Map Transaction Amount",total_amount)
            elif selected=="User":
                col1,col2=st.columns([3,1])
                with col1:
                    years= st.slider("Select The Year",map_user["Year"].min(), map_user["Year"].max(),map_user["Year"].min())
                    fig2 = px.choropleth(
                        map_user_amount_count_Y(map_user, years),
                        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                        featureidkey='properties.ST_NM',
                        locations='State',
                        color='Registered_Users',
                        color_continuous_scale='Viridis',
                        hover_data={
                            'Registered_Users': True, 
                            'AppOpens':True,
                            'State': True              
                        }  
                    )
                    fig2.update_layout(
                                title={
                                    "text": "Map user data",
                                    "x": 0.3,  # Center the title
                                    "xanchor": "center",
                                    "yanchor": "top"
                                },)
                    fig2.update_geos(fitbounds="locations", visible=False)
                    st.plotly_chart(fig2, use_container_width=False) 
                with col2:
                    st.markdown(
                            """
                            <style>
                            div[data-testid="stMetricValue"] {
                                font-size: 25px; /* Change the font size */
                                /*color: #000000 ; */ /* Optional: Change font color */
                            }
                            </style>
                            """,
                            unsafe_allow_html=True,
                        )
                    Total_user_count_map=map_user['Registered_Users'].sum()
                    st.metric(label="Total Map User Count", value=Total_user_count_map) 
                    Total_Appopens_map=map_user['AppOpens'].sum()
                    st.metric(label="Total AppOpens", value=Total_Appopens_map) 
        elif selected_section=="Top data":
            selected=st.selectbox("select",["Transaction","User"])
            if selected=="Transaction":
                col1,col2=st.columns([3,1])
                with col1:
                    years= st.slider("Select The Year",top_trans["Year"].min(), top_trans["Year"].max(),top_trans["Year"].min())   
                    generate_choropleth(Transaction_amount_count_Y(top_trans, years),"Top Transaction data")
                with col2:
                    st.markdown(
                            """
                            <style>
                            div[data-testid="stMetricValue"] {
                                font-size: 25px; /* Change the font size */
                                /*color: #000000 ; */ /* Optional: Change font color */
                            }
                            </style>
                            """,
                            unsafe_allow_html=True,
                        )
                    total_count,total_amount=calculate_totals(top_trans)
                    st.metric("Total Top Transaction Count",total_count)
                    st.metric("Total Top Transaction Amount",total_amount)
            elif selected=="User":
                col1,col2=st.columns([3,1])
                with col1:
                    years= st.slider("Select The Year",top_user["Year"].min(), top_user["Year"].max(),top_user["Year"].min())
                    fig3 = px.choropleth(
                        top_user_amount_count_Y(top_user, years),
                        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                        featureidkey='properties.ST_NM',
                        locations='State',
                        color='Registered_Users',
                        color_continuous_scale='Viridis',
                        hover_data={
                            'Registered_Users': True,  # Show Transaction_amount in hover
                            'State': True               # Optionally hide State if not needed
                        }  
                    )
                    fig3.update_layout(
                                title={
                                    "text": "Top user data",
                                    "x": 0.3,  # Center the title
                                    "xanchor": "center",
                                    "yanchor": "top"
                                },)
                    fig3.update_geos(fitbounds="locations", visible=False)
                    st.plotly_chart(fig3, use_container_width=False) 
                with col2:
                    st.markdown(
                            """
                            <style>
                            div[data-testid="stMetricValue"] {
                                font-size: 25px; /* Change the font size */
                                /*color: #000000 ; */ /* Optional: Change font color */
                            }
                            </style>
                            """,
                            unsafe_allow_html=True,
                        )
                    Total_user_count_top=top_user['Registered_Users'].sum()
                    st.metric("Total Top User Count",Total_user_count_top) 
elif selected == "Data Insights":
    query_question = st.selectbox("Select Query", [
        "1.Top 10 States with Highest Transaction amount",
        "2.Top 10 States with least Transaction amount",
        "3.Top 10 mobile brands used",
        "4.Top 10 districts with Highest transaction amount",
        "5.Top 10 states with Appopens",
        "6.States with least Appopens",
        "7.Top 10 pincodes with Highest transaction amount",
        "8.Top 10 pincodes with least transaction amount",
        "9.Top 10 pincodes with Highest registered users",
        "10.Top 10 pincodes with least registered users"])
    if query_question=="1.Top 10 States with Highest Transaction amount":
       color=["#6F36AD"]
       fig_1= px.bar(query1(), x= "State", y= "Transaction_amount",title= "TOP 10 STATES WITH HIGHEST TRANSACTION AMOUNT IN AGGREGATE DATA",
                     color_discrete_sequence=color)
       st.plotly_chart(fig_1)
    elif query_question=="2.Top 10 States with least Transaction amount":
         color=["#6F36AD"]
         fig_2= px.bar(query2(), x= "State", y= "Transaction_amount",title= "STATES WITH LEAST TRANSACTION AMOUNT IN AGGREGATE DATA",
                       color_discrete_sequence=color)
         st.plotly_chart(fig_2)
    elif query_question=="3.Top 10 mobile brands used":
         fig_3=px.pie(query3(), values= "User_count", names= "Brand", color_discrete_sequence=px.colors.sequential.Agsunset,
                       title= "TOP MOBILE BRANDS USER COUNT")
         st.plotly_chart(fig_3)
    elif query_question=="4.Top 10 districts with Highest transaction amount":
         fig_4=px.pie(query4(), values= "Transaction_amount", names= "District", color_discrete_sequence=px.colors.sequential.Agsunset,
                       title= "TOP 10 DISTRICTS WITH HIGHEST TRANSACTION AMOUNT")
         st.plotly_chart(fig_4)
    elif query_question=="5.Top 10 states with Appopens":
         color=["#6F36AD"]
         fig_5=px.bar(query5(), x= "State", y= "AppOpens",title= "TOP 10 STATES WITH APPOPENS",
                       color_discrete_sequence=color)
         st.plotly_chart(fig_5)
    elif query_question=="6.States with least Appopens":
         color=["#6F36AD"]
         fig_6=px.bar(query6(), x= "State", y= "AppOpens",title= "STATES WITH LEAST APPOPENS",
                       color_discrete_sequence=color)
         st.plotly_chart(fig_6)
    elif query_question=="7.Top 10 pincodes with Highest transaction amount":
         color=["#6F36AD"]
         fig_7=px.bar(query7(), x= "Pincode", y= "Transaction_amount",title= "TOP 10 PINCODES WITH HIGHEST TRANSACTION AMOUNT",
                       color_discrete_sequence=color)
         fig_7.update_xaxes(type="category")
         st.plotly_chart(fig_7)
    elif query_question=="8.Top 10 pincodes with least transaction amount":
         color=["#6F36AD"]
         fig_8=px.bar(query8(), x= "Pincode", y= "Transaction_amount",title= "TOP 10 PINCODES WITH LEAST TRANSACTION AMOUNT",
                       color_discrete_sequence=color)
         fig_8.update_xaxes(type="category")
         st.plotly_chart(fig_8)
    elif query_question=="9.Top 10 pincodes with Highest registered users":
         color=["#6F36AD"]
         fig_9=px.bar(query9(), x= "Pincode", y= "Registered_Users",title= "TOP 10 PINCODES WITH HIGHEST REGISTERED USERS",
                       color_discrete_sequence=color)
         fig_9.update_xaxes(type="category")
         st.plotly_chart(fig_9)
    elif query_question=="10.Top 10 pincodes with least registered users":
         color=["#6F36AD"]
         fig_10=px.bar(query10(), x= "Pincode", y= "Registered_Users",title= "TOP 10 PINCODES WITH LEAST REGISTERED USERS",
                       color_discrete_sequence=color)
         fig_10.update_xaxes(type="category")
         st.plotly_chart(fig_10)



        
            
                
                
                