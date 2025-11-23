#requires -Version 5
 = "Data Source=C:\src\gocluster\data\records\spots.db;Version=3"
Add-Type -AssemblyName System.Data
 = New-Object System.Data.SQLite.SQLiteConnection()
.Open()
 = .CreateCommand()
.CommandText = "SELECT DISTINCT source_type, source_node FROM spot_records ORDER BY source_type"
 = .ExecuteReader()
while (.Read()) {
     = .GetString(0)
     = if (.IsDBNull(1)) { "" } else { .GetString(1) }
    Write-Output "source_type= source_node="
}
.Close()
.Close()
