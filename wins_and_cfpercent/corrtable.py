import pandas as pd


df1 = pd.read_csv(
    r"/Users/ericwiniecke/Documents/github/cost_cup/wins_and_cfpercent/team_wins_and_cfpercent_2016.csv"
)
df2 = pd.read_csv(
    r"/Users/ericwiniecke/Documents/github/cost_cup/wins_and_cfpercent/team_wins_and_cfpercent_2017.csv"
)
df3 = pd.read_csv(
    r"/Users/ericwiniecke/Documents/github/cost_cup/wins_and_cfpercent/team_wins_and_cfpercent_2018.csv"
)
df = pd.concat([df1, df2, df3], axis=0)
df = df.reset_index(drop=True)
df["Total_Payroll"] = (
    df["Total_Payroll"].str.replace("[\$,]", "", regex=True).astype(int)
)
correlation_table = df.drop(columns=["Abbreviation"]).corr()
print(correlation_table)
