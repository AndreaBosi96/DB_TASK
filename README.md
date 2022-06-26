# My solution

### How to run
To run the solution locally:
- import the db in your sql server
- update the db connection string in appsettings.json according to your system
- install necessary packages:
        -- Microsoft.Extensions.Configuration
        -- System.Data.SqlClient
- get into the project folder and run simple dotnet cmds
        -- dotnet restore
        -- dotnet run

Please note that the input file (after "Headers read") might take a couple of minutes to run. It is expecially due to a check about GUIDS.
With more time, I would have probably made the process faster using a different solution (maybe a hash set).


### How to improve
- Main problem is performance issue when importing data and doing checks
- Better exceptions handling
- Logging instead of prints!
- Better code modularization
- Increase amount and quality of tests


# Thanks for reading!

