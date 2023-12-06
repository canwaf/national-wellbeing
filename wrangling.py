import pandas as pd

df = pd.read_excel("/Users/abdulkasim/Documents/data_ingestion_projects/national-wellbeing/ukmeasuresofnationalwellbeingnov2023.xlsx", sheet_name="5.6_Feeling_safe")

df1 = pd.concat([df.iloc[18:29], df.iloc[32:]], axis=0).reset_index(drop=True)

def wrangle(df):
    df.columns = ["Dimension", "Male %", 'Male LCL %', 'Male UCL %', 'Male sample size',
       'Female %', 'Female LCL %', 'Female UCL %', 'Female sample size']
    df["Variable Name"] = "Percentage of people who felt fairly or very safe walking alone in their local area after dark by sex, Apr 2022 to Mar 2023 by country, English region and age"
    df = df[['Variable Name', 'Dimension', 'Male %', 'Male LCL %', 'Male UCL %', 'Male sample size',
       'Female %', 'Female LCL %', 'Female UCL %', 'Female sample size']]
    return df

df = wrangle(df1)

df["Unit"] = "Percentage"

df["refPeriod"] = df["Dimension"].str.startswith("Apr").values
df.loc[df['refPeriod'], 'refPeriod'] = df.loc[df['refPeriod'], 'Dimension']
df['refPeriod'] = df.apply(lambda row: row['Dimension'] if row['refPeriod'] else '', axis=1)

df["refArea"] = ~df["Dimension"].str.startswith("A").values
df.loc[df['refArea'], 'refArea'] = df.loc[df['refArea'], 'Dimension']
df['refArea'] = df.apply(lambda row: row['Dimension'] if row['refArea'] else '', axis=1)

df["Age"] = df["Dimension"].str.startswith("Aged").values
df.loc[df['Age'], 'Age'] = df.loc[df['Age'], 'Dimension']
df['Age'] = df.apply(lambda row: row['Dimension'] if row['Age'] else '', axis=1)

df = df[['Variable Name', 'refPeriod', 'refArea', 'Age', 'Male %', 'Male LCL %', 'Male UCL %', 'Male sample size',
       'Female %', 'Female LCL %', 'Female UCL %', 'Female sample size', "Unit"]]

df["refPeriod"] = df.apply(lambda x: x["refPeriod"][4:8] + "-04-01/P1Y" if x["refPeriod"] != "" else "", axis=1)

df["Observation Status"] = df.apply(lambda x: "x" if "[x]" in str(x["Male %"]) else "", axis=1)

df = pd.DataFrame([[str(element).replace('[x]', '') for element in row] for row in df.values], columns=df.columns)

df["refPeriod"] = df.apply(lambda x: "2022-04-01/P1Y" if x["refPeriod"] == "" else x["refPeriod"], axis=1)
df["refArea"] = df.apply(lambda x: "England and Wales" if x["refArea"] == "" else x["refArea"], axis=1)
df["Age"] = df.apply(lambda x: "Aged 16 years and over" if x["Age"] == "" else x["Age"], axis=1)
melted_df = pd.melt(df, id_vars=['Variable Name', 'refPeriod', 'refArea', 'Age', 'Unit', 'Observation Status'], var_name='Sex', value_name='Value')

pattern = r'(?P<Sex>Male|Female)\s+(?P<Metric>.*)'
melted_df[["Sex", "Metric"]] = melted_df['Sex'].str.extract(pattern)

#df_pivoted = pd.pivot_table(melted_df, values='Value', columns='Metric', aggfunc='first')
df_pivoted = melted_df.set_index(['Variable Name', 'refPeriod', 'refArea', 'Age', 'Unit',
       'Observation Status', 'Sex', 'Metric'])['Value'].unstack().reset_index()

df = df_pivoted[['refArea', 'Variable Name', 'refPeriod', 'Age', 'Sex', '%', 'Unit', 'sample size', 
      'LCL %', 'UCL %', 'Observation Status']]
df = df.rename(columns={'%': 'Value', 'sample size': 'Sample Size', 'LCL %': 'Lower Confidence Interval (95%)', 'UCL %': 'Upper Confidence Interval (95%)'})
df.columns.name = None

df.to_csv("feeling-safe.csv")


