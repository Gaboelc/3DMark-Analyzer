import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

app_info_df = pd.read_csv('./data/app_info.csv')
cpu_df = pd.read_csv('./data/cpu.csv')
gpu_df = pd.read_csv('./data/gpu.csv')
hardware_info_df = pd.read_csv('./data/hardware_info.csv')
results_df = pd.read_csv('./data/results.csv')
results_target_df = pd.read_csv('./data/results_target.csv')
test_info_df = pd.read_csv('./data/test_info.csv')
workload_sets_df = pd.read_csv('./data/workload_sets.csv')

st.title("3DMark Data Analysis Dashboard")

# CPU Data Section
st.header("CPU Data Analysis")
if not cpu_df.empty:
    time = cpu_df['BM_RunTimeSecondsE']
    temp_columns = [col for col in cpu_df.columns if 'Temperature' in col]
    freq_columns = [col for col in cpu_df.columns if 'ClockFrequency' in col]
    temp_stats = cpu_df[temp_columns].describe()
    
    throttling_info = []

    if throttling_info:
        for col, times in throttling_info:
            st.subheader(f"Throttling Events for {col}")
            st.write("Significant reduction detected at the following times (in seconds):")
            st.dataframe(times.reset_index(drop=True))
            
            st.subheader(f"Throttling Chart for {col}")
            fig, ax = plt.subplots(figsize=(15, 5))
            ax.plot(cpu_df['BM_RunTimeSecondsE'], cpu_df[col], label=f'{col} - Frequency', color='blue')
            ax.scatter(times, cpu_df.loc[times.index, col], color='red', label='Throttling Events', zorder=5)
            ax.set_xlabel('Time (Seconds)')
            ax.set_ylabel('Clock Frequency (MHz)')
            ax.set_title(f'Throttling Events in {col} Over Time')
            ax.legend()
            st.pyplot(fig)
    else:
        st.write("No significant throttling events detected in CPU cores.")
    
    # Analysis of unusually high temperatures
    if temp_columns:
        # (IQR method)
        high_temp_threshold = temp_stats.loc['75%'] + 1.5 * (temp_stats.loc['75%'] - temp_stats.loc['25%'])
        outlier_cores = high_temp_threshold[high_temp_threshold > temp_stats.loc['max']]

        st.header("Cores with Unusually High Temperatures")
        if not outlier_cores.empty:
            st.write("Cores with temperatures exceeding the high temperature threshold:")
            st.dataframe(outlier_cores)
        else:
            st.write("No cores with unusually high temperatures found.")
    else:
        st.write("No temperature columns found in CPU data.")
    
    # Line chart for core temperatures
    st.subheader("CPU Temperature Line Chart")
    fig, ax = plt.subplots(figsize=(15, 7))
    for col in temp_columns:
        ax.plot(time, cpu_df[col], label=col)
    ax.set_xlabel('Time (Seconds)')
    ax.set_ylabel('Temperature (°C)')
    ax.set_title('CPU Core Temperatures Over Time')
    ax.legend()
    st.pyplot(fig)

    # Heatmap to visualize temperatures
    st.subheader("Heatmap of CPU Core Temperatures")
    temp_data = cpu_df[temp_columns].transpose()

    fig, ax = plt.subplots(figsize=(10, 6))
    sns.heatmap(temp_data, cmap='coolwarm', ax=ax)
    ax.set_title('Heatmap of CPU Core Temperatures')
    st.pyplot(fig)
    
    # CPU Clock Frequencies Over Time
    st.subheader("CPU Clock Frequency Line Chart")
    fig, ax = plt.subplots(figsize=(15, 7))
    for col in freq_columns:
        ax.plot(time, cpu_df[col], label=col)
    ax.set_xlabel('Time (Seconds)')
    ax.set_ylabel('Clock Frequency (MHz)')
    ax.set_title('CPU Core Clock Frequencies Over Time')
    ax.legend()
    st.pyplot(fig)
    
    if temp_columns:
        cpu_df['Avg_Temperature'] = cpu_df[temp_columns].mean(axis=1)
        cpu_df['Max_Temperature'] = cpu_df[temp_columns].max(axis=1)

        st.header("Average and Maximum CPU Temperatures Over Time")
        time = cpu_df['BM_RunTimeSecondsE']

        st.subheader("Average and Maximum CPU Temperatures Line Chart")
        fig, ax = plt.subplots(figsize=(15, 7))
        ax.plot(time, cpu_df['Avg_Temperature'], label='Average Temperature', color='blue')
        ax.plot(time, cpu_df['Max_Temperature'], label='Maximum Temperature', color='red')
        ax.set_xlabel('Time (Seconds)')
        ax.set_ylabel('Temperature (°C)')
        ax.set_title('Average and Maximum CPU Core Temperatures Over Time')
        ax.legend()
        st.pyplot(fig)
    else:
        st.write("No temperature columns found in CPU data.")

# GPU Data Section
st.header("GPU Data Analysis")

# Hardware Information
st.header("Hardware Information")
if not hardware_info_df.empty:
    st.write("Hardware information preview:")
    st.dataframe(hardware_info_df)

# Application Information
st.header("Application Information")
if not app_info_df.empty:
    st.write("Application information preview:")
    st.dataframe(app_info_df)

# Test and Workload Information
st.header("Test and Workload Information")
if not test_info_df.empty:
    st.write("Test information preview:")
    st.dataframe(test_info_df)

if not workload_sets_df.empty:
    st.write("Workloads preview:")
    st.dataframe(workload_sets_df)
