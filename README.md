# sythentic_data
Using Nvidia Nemo data designer to create a dataset, potentially web app and digital twin


Gameplan:
Use the nvidia data designer nemotron sdk to create a dataset, will start small

https://build.nvidia.com/nemo/data-designer

read in karthpathy md for agentic development

Next potentially load into a sql server and develop azure function to interact with a web app.

February 4
Spinning up Azure resource group
Going to a databricks environment
Going to setup ADLS so we can store the sythentic data we create
Then have an autoloader pipeline take the json files, transform them and write as delta tables 

Will then need to create an init script and odbc driver so we can insert into the database. 

Pulled in repo to databricks. 
Need to work through authentication of DBX to ADLS and can then write files there. 