### Data Dictionary of Facts and dimension tables

### Temperature information per City - Dim_Temperature

year: integer (nullable = true) - Temperature Year
month: integer (nullable = true) - Temperture Month
AverageTemperature: double (nullable = true) - Average Temperature per city
temp_city: string (nullable = true) - Name of City
country: string (nullable = true) - Name of Country (Only United States)


### Immigration information per City - Dim_Immigration
cicid: double (nullable = true) - ID of person who was immigrated
year: integer (nullable = true) - Year of immigration
month: integer (nullable = true) -  Month of immigration
origin_country: string (nullable = true) - Country of origin
destination_airport_code: string (nullable = true) -  String - 3 character port code of Destination
destination_us_city: string (nullable = true) - Name of destination city


### Demograpic information per City - Dim_Demograhy
City: string (nullable = true) - Name of US City
median_age: float (nullable = true) - Median Age per city
percent_Male_Population: double (nullable = true) - Percentage of Male population in total population
percent_Female_Population: double (nullable = true) - Percentage of Female population in total population
percent_veterans: double (nullable = true) - Percentage of veterans population in total population
percent_foreigners: double (nullable = true) - Percentage of foreigners population in total population
natives: double (nullable = true) - Percentage of Native Americans in total population
Asian: double (nullable = true) - Percentage of Asians in total population
Black: double (nullable = true) -Percentage of Black population in total population
hispanic_or_latino: double (nullable = true) - Percentage of hispanic or latino population in total population
White: double (nullable = true) - Percentage of white population in total population


### Demograpic information per City - Dim_City
us_state_code: string (nullable = true) - 2 Character code for each state in US
us_state: string (nullable = true) - Name of State in US
us_city: string (nullable = true) - Name of City in US


### Information about Immigration trend - Fact_Immigration
year_of_immigration: integer (nullable = true) - Year from dimension table Dim_Immigration
month_of_immigration: integer (nullable = true) - Month from dimension table Dim_Immigration
origin_country: string (nullable = true) -Country of origin from dimension table Dim_Immigration
destination_state: string (nullable = true) -US State from dimension table Dim_City
destination_city: string (nullable = true) - US City from dimension table Dim_Immigration
Avg_temp_city: double (nullable = true) - Average temperature per city from dimension table Dim_Temperature
percent_foreigners: double (nullable = true) - Percentage of foreigners in total population Dim_Demograhy
natives: double (nullable = true) - Percentage of native amreicans in total population Dim_Demograhy
Asian: double (nullable = true) - Percentage of Asians in total population Dim_Demograhy
Black: double (nullable = true) - Percentage of Black in total population Dim_Demograhy
hispanic_or_latino: double (nullable = true) - Percentage of hispanic or latino in total population Dim_Demograhy
White: double (nullable = true) - Percentage of white in total population Dim_Demograhy
count_immi_per_city: long (nullable = false) -Count of immigration per city per month and year from origin country

