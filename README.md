# My solution

### How to run
To run the solution locally:
- import the db in your sql server
- update the db connection string in appsettings.json according to your system
- install necessary packages:
        -- Microsoft.Extensions.Configuration
        -- Microsoft.Extensions.Configuration.json
        -- System.Data.SqlClient
- get into the project folder and run simple dotnet cmds
        -- dotnet restore
        -- dotnet run

### Files
- program.cs is the source code
- appsettings.json for settings
- DB_ASK.csproj is the project file (you can find required packages here!)
- Other: csv input, xml output, assignment's instructions, .txt instructions (I also uploaed dlls and so on to be sure)

### My approach
- Setup some initial stuff
- Read the csv: while reading perform most of the checks and tweaks dividing rows in valid and invalid datatables.
- Insert the dts to db
- Export the dts to xml files
- Perform some simple tests on dts
- Execute stored procedures
- 
### How to improve
- Better exceptions handling
- Logging instead of prints! I did not have a lot of time to put into this task and simple prints did the job, but it would be better to use some logger.
- Better code modularization: a couple of classes could probably be merged together
- Increase amount and quality of tests. I just tested the datatables in a very basic way. Introduce some tests on DB and on XML files.
- Introduce unit tests to try functions and classes without running the whole code everytime (on the whole dataset)
- Some functions could probably be developed in a more performig way
- Output: AS IS, the results are only in the console. I also printed the result of the procedures, but the format is not really user-friendly. For a real case, something like an output csv or txt might be produced!

### To conclude
Thanks for reading this file and checking my solution. Feel free to contact me for anything! 


# Thanks for reading!

