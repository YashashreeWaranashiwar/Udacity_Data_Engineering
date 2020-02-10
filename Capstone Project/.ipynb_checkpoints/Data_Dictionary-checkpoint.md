### Data Dictionary of Facts and dimension tables

### Temperature information per state - Dim_Temperature

year : Integer - Temperature Year
month : Integer - Temperture Month
us_state : String - Name of State
us_state_code : String - Code of State
Country : String - Country (Value - United States)
avg_state_temp : Double - Average Temperature per state in Celcius


### Immigration information per state - Dim_Immigration
year : Integer - Year of immigration
month : Integer - Month of immigration
origin_country : String - Country of origin
destination_port_code : String - 3 character port code of Destination
destination_us_city : String - Name of destination city
us_state_code : String - 2 character state code of state
us_state : String - Name of state


### Demograpic information per state - Dim_Demograhy
State : String - Name of state
state_code : String - 2 character state code of state
Median_age : Float - Median Age per state
percent_Male_Population : Double - Percentage of Male population in total population
percent_Female_Population : Double - Percentage of Female population in total population
percent_veterans : Double - Percentage of veterans population in total population
percent_foreigners : Double - Percentage of foreigners population in total population


### Demograpic information per state - Dim_Demograhy