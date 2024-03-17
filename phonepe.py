# import streamlit as st
# from streamlit_option_menu import option_menu
import PIL
from PIL import Image
import json
import streamlit as st
import pandas as pd
import requests
import mysql.connector
import pymysql
import plotly.express as px
import plotly.graph_objects as go
from streamlit_option_menu import option_menu

# Srteamlit part
icon = Image.open(r"C:\Users\Aarthi\Pictures\phonepe-img.png")
st.set_page_config(page_title= "Phonepe Pulse Data Visualization",
                   page_icon= icon,
                   layout= "wide",
                   initial_sidebar_state= "expanded")
# st.set_page_config(layout= "wide")
st.title("PHONEPE PULSE AND DATA VISUALIZATION")

with st.sidebar:
    st.header(":violet[**PhonePe Pulse Data**]")
    selected = option_menu(None,
                            options=["Home","Statewise-Insights","Transactions-Insights","Users-Insights"],
                            default_index=0,
                            orientation="horizontal",
                            styles={"container": {"width": "90%"},
                                    "options": {"margin": "10px"},
                                    "nav-link": {"font-size": "20px", "text-align": "center", "margin": "15px", "--hover-color": "#6F36AD"},
                                    "nav-link-selected": {"background-color": "#6F36AD"}})

    
if selected == "Home":
    im1 = Image.open(r"C:\Users\Aarthi\Pictures\phonepe-img.png")
    # im2 = Image.open("")
    st.image(im1, width=500)
    # st.image(im2)
   
    st.subheader("PhonePe is a mobile payment platform using which you can transfer money using UPI, recharge phone numbers, pay utility bills, etc. PhonePe works on the Unified Payment Interface (UPI).")
    st.subheader(":Red[➡️TECHNOLOGIES]")
    st.write("****👾****")
    st.write("****✔️Github Cloning****")
    st.write("****✔️Pandas****")
    st.write("****✔️MYSQL****")
    st.write("****✔️Streamlit Part****")
    st.write("****✔️Plotly****")
    st.write("****👾****")


# Aggre_Transation_df

def Aggre_Transaction_type(df, state):
    df_state= df[df["States"] == state]
    df_state.reset_index(drop= True, inplace= True)

    agttg= df_state.groupby("Transaction_type")[["Transaction_count", "Transaction_amount"]].sum()
    agttg.reset_index(inplace= True)

    col1,col2= st.columns(2)
    with col1:

        fig_hbar_1= px.bar(agttg, x= "Transaction_count", y= "Transaction_type", orientation="h",
                        color_discrete_sequence=px.colors.sequential.Aggrnyl, width= 600, 
                        title= f"{state.upper()} TRANSACTION TYPES AND TRANSACTION COUNT",height= 500)
        st.plotly_chart(fig_hbar_1)

    with col2:

        fig_hbar_2= px.bar(agttg, x= "Transaction_amount", y= "Transaction_type", orientation="h",
                        color_discrete_sequence=px.colors.sequential.Greens_r, width= 600,
                        title= f"{state.upper()} TRANSACTION TYPES AND TRANSACTION AMOUNT", height= 500)
        st.plotly_chart(fig_hbar_2)







































