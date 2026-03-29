import datetime as dt
import os
import json
import requests
from datetime import datetime, timedelta
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ---------------------------------------------------
# SETUP: Define directories and Bash commands
# ---------------------------------------------------
WorkingDirectory = os.path.expanduser("~/airflow")
LogFiles = WorkingDirectory + "/LogFiles/"
StagingArea = WorkingDirectory + "/StagingArea/"
StarSchema = WorkingDirectory + "/StarSchema/"

# Bash commands to create unique IP and Date lists and to copy the Fact table to the StarSchema folder
uniqIPsCommand = "sort -u " + StagingArea + "RawIPAddresses.txt > " + StagingArea + "UniqueIPAddresses.txt"
uniqDatesCommand = "sort -u " + StagingArea + "RawDates.txt > " + StagingArea + "UniqueDates.txt"
copyFactTableCommand = "cp " + StagingArea + "FactTable.txt " + StarSchema + "FactTable.txt"

# Ensure required directories exist
for folder in [WorkingDirectory, LogFiles, StagingArea, StarSchema]:
    try:
        os.mkdir(folder)
    except Exception:
        pass

# ---------------------------------------------------
# EXTRACTION FUNCTIONS
# ---------------------------------------------------
def CopyDataFromLogFileIntoStagingArea(nameOfLogFile):
    print('Copying content from log file', nameOfLogFile)
    suffix = nameOfLogFile[-3:]
    if suffix == "log":
        # Open output files for 14- and 18-column data
        OutputFileFor14ColData = open(StagingArea + 'OutputFor14ColData.txt', 'a')
        OutputFileFor18ColData = open(StagingArea + 'OutputFor18ColData.txt', 'a')
        InFile = open(LogFiles + nameOfLogFile, 'r')
        Lines = InFile.readlines()
        for line in Lines:
            if line[0] != "#":
                Split = line.split(" ")
                if len(Split) == 14:
                    OutputFileFor14ColData.write(line)
                elif len(Split) == 18:
                    OutputFileFor18ColData.write(line)
                else:
                    print("Fault: unrecognised column number " + str(len(Split)))
        OutputFileFor14ColData.close()
        OutputFileFor18ColData.close()
        InFile.close()

def EmptyOutputFilesInStagingArea():
    open(StagingArea + 'OutputFor14ColData.txt', 'w').close()
    open(StagingArea + 'OutputFor18ColData.txt', 'w').close()

def CopyLogFilesToStagingArea():
    arr = os.listdir(LogFiles)
    if not arr:
        print('No files found in Log Files folder')
    EmptyOutputFilesInStagingArea()
    for f in arr:
        CopyDataFromLogFileIntoStagingArea(f)

# ---------------------------------------------------
# TRANSFORMATION FUNCTIONS: Build Fact Table
# ---------------------------------------------------
def Add14ColDataToFactTable():
    """
    Process 14-column log files.
    Expected fields (indices):
      0: Date, 1: Time, 3: cs-method, 4: cs-uri-stem, 8: c-ip,
      9: cs(User-Agent), 10: sc-status, 13: time-taken.
    For 14-col logs, Referer, sc_bytes, and cs_bytes are not available.
    """
    InFile = open(StagingArea + 'OutputFor14ColData.txt', 'r')
    OutFact = open(StagingArea + 'FactTable.txt', 'a')
    Lines = InFile.readlines()
    for line in Lines:
        line = line.strip()
        if not line:
            continue

        Split = line.split(" ")
        if len(Split) < 14:
            print("Skipping malformed 14-column row:", line)
            continue

        Browser = Split[9].replace(",", "")

        # Build a row with empty fields for Referer, sc_bytes, cs_bytes
        OutputLine = (
            Split[0] + "," +        # Date
            Split[1] + "," +        # Time
            Split[3] + "," +        # Method
            Split[4] + "," +        # URIStem
            Split[8] + "," +        # IP
            Browser + "," +         # UserAgent
            "" + "," +              # Referer (not available)
            Split[10] + "," +       # Status
            "" + "," +              # sc_bytes (not available)
            "" + "," +              # cs_bytes (not available)
            Split[13] + "\n"        # TimeTaken
        )
        OutFact.write(OutputLine)
    InFile.close()
    OutFact.close()

def Add18ColDataToFactTable():
    """
    Process 18-column log files.
    Expected fields (indices):
      0: Date, 1: Time, 3: cs-method, 4: cs-uri-stem, 8: c-ip,
      9: cs(User-Agent), 11: cs(Referer), 12: sc-status,
      15: sc-bytes, 16: cs-bytes, 17: time-taken.
    """
    InFile = open(StagingArea + 'OutputFor18ColData.txt', 'r')
    OutFact = open(StagingArea + 'FactTable.txt', 'a')
    Lines = InFile.readlines()
    for line in Lines:
        line = line.strip()
        if not line:
            continue

        Split = line.split(" ")
        if len(Split) < 18:
            print("Skipping malformed 18-column row:", line)
            continue

        Browser = Split[9].replace(",", "")
        Referer = Split[11].replace(",", "")

        OutputLine = (
            Split[0] + "," +        # Date
            Split[1] + "," +        # Time
            Split[3] + "," +        # Method
            Split[4] + "," +        # URIStem
            Split[8] + "," +        # IP
            Browser + "," +         # UserAgent
            Referer + "," +         # Referer
            Split[12] + "," +       # Status
            Split[15] + "," +       # sc_bytes
            Split[16] + "," +       # cs_bytes
            Split[17] + "\n"        # TimeTaken
        )
        OutFact.write(OutputLine)
    InFile.close()
    OutFact.close()

def BuildFactTable():
    """
    Builds the Fact table by writing a header and then appending data
    from both 14-col and 18-col log files.
    Fact table schema: Date,Time,Method,URIStem,IP,UserAgent,Referer,Status,sc_bytes,cs_bytes,TimeTaken
    """
    with open(StagingArea + 'FactTable.txt', 'w') as file:
        file.write("Date,Time,Method,URIStem,IP,UserAgent,Referer,Status,sc_bytes,cs_bytes,TimeTaken\n")
    Add14ColDataToFactTable()
    Add18ColDataToFactTable()

# ---------------------------------------------------
# DIMENSION EXTRACTION FUNCTIONS (Optional)
# ---------------------------------------------------
def getIPsFromFactTable():
    InFile = open(StagingArea + 'FactTable.txt', 'r')
    OutputFile = open(StagingArea + 'RawIPAddresses.txt', 'w')
    Lines = InFile.readlines()
    firstLine = True
    for line in Lines:
        if firstLine:
            firstLine = False
            continue

        line = line.strip()
        if not line:
            continue

        Split = line.split(",")

        if len(Split) > 4:
            IPAddr = Split[4] + "\n"
            OutputFile.write(IPAddr)
        else:
            print("Skipping malformed row:", line)

    InFile.close()
    OutputFile.close()

def getDatesFromFactTable():
    InFile = open(StagingArea + 'FactTable.txt', 'r')
    OutputFile = open(StagingArea + 'RawDates.txt', 'w')
    Lines = InFile.readlines()
    firstLine = True
    for line in Lines:
        if firstLine:
            firstLine = False
        else:
            Split = line.split(",")
            DateInfo = Split[0] + "\n"
            OutputFile.write(DateInfo)
    InFile.close()
    OutputFile.close()

def makeDateDimension():
    InDateFile = open(StagingArea + 'UniqueDates.txt', 'r')
    with open(StarSchema + 'DimDateTable.txt', 'w') as f:
        f.write("Date,Year,Month,Day,DayofWeek\n")
    Lines = InDateFile.readlines()
    Days = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    for line in Lines:
        line = line.strip()
        if len(line) > 0:
            try:
                date = datetime.strptime(line, "%Y-%m-%d").date()
                weekday = Days[date.weekday()]
                out = f"{date},{date.year},{date.month},{date.day},{weekday}\n"
                with open(StarSchema + 'DimDateTable.txt', 'a') as f:
                    f.write(out)
            except:
                print("Error with Date:", line)
    InDateFile.close()

def makeLocationDimension():
    InFile = open(StagingArea + 'UniqueIPAddresses.txt', 'r')
    with open(StarSchema + 'DimIPLoc.txt', 'w') as f:
        f.write("IP,country_code,country_name,city,lat,long\n")
    Lines = InFile.readlines()
    for line in Lines:
        line = line.strip()
        if len(line) > 0:
            request_url = 'https://geolocation-db.com/jsonp/' + line
            try:
                response = requests.get(request_url)
                result = response.content.decode()
            except:
                print("Error response from geolocation API for IP:", line)
                continue
            try:
                result = result.split("(")[1].strip(")")
                result = json.loads(result)
                outputLine = f"{line},{result.get('country_code','')},{result.get('country_name','')},{result.get('city','')},{result.get('latitude','')},{result.get('longitude','')}\n"
                with open(StarSchema + 'DimIPLoc.txt', 'a') as f:
                    f.write(outputLine)
            except Exception as e:
                print("Error processing location for IP:", line, e)
    InFile.close()

# ---------------------------------------------------
# LOAD FUNCTION: Load Fact Table into SQLite
# ---------------------------------------------------
def load_data_to_sqlite():
    import sqlite3

    db_path = WorkingDirectory + "/etl.db"
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Enable foreign key support
    cur.execute("PRAGMA foreign_keys = ON;")

    # Drop the tables so it recreates cleanly
    cur.execute("DROP TABLE IF EXISTS FactTable")
    cur.execute("DROP TABLE IF EXISTS DimDateTable")
    cur.execute("DROP TABLE IF EXISTS DimLocationTable")
    conn.commit()

    # -----------------------------
    # DATE DIMENSION TABLE
    # -----------------------------
    cur.execute("""
        CREATE TABLE DimDateTable (
            Date TEXT PRIMARY KEY,
            Year INTEGER,
            Month INTEGER,
            Day INTEGER,
            DayofWeek TEXT
        )
    """)
    conn.commit()

    dim_date_file_path = StarSchema + "DimDateTable.txt"
    dim_date_rows = []

    with open(dim_date_file_path, 'r') as f:
        next(f)  # Skip header row
        for line in f:
            line = line.strip()
            if not line:
                continue

            split_row = line.split(",")

            if len(split_row) == 5:
                try:
                    dim_date_rows.append((
                        split_row[0],       # Date
                        int(split_row[1]),  # Year
                        int(split_row[2]),  # Month
                        int(split_row[3]),  # Day
                        split_row[4]        # DayofWeek
                    ))
                except ValueError:
                    print("Skipping malformed DimDateTable numeric row:", line)
            else:
                print("Skipping malformed DimDateTable row with", len(split_row), "fields:", line)

    unique_dim_date_rows = {}
    for row in dim_date_rows:
        date = row[0]
        if date not in unique_dim_date_rows:
            unique_dim_date_rows[date] = row

    dim_date_rows = list(unique_dim_date_rows.values())

    cur.executemany("""
        INSERT INTO DimDateTable (
            Date, Year, Month, Day, DayofWeek
        )
        VALUES (?, ?, ?, ?, ?)
    """, dim_date_rows)
    conn.commit()

    # -----------------------------
    # LOCATION DIMENSION TABLE
    # -----------------------------
    cur.execute("""
        CREATE TABLE DimLocationTable (
            IP TEXT PRIMARY KEY,
            country_code TEXT,
            country_name TEXT,
            city TEXT,
            lat REAL,
            long REAL
        )
    """)
    conn.commit()

    dim_loc_file_path = StarSchema + "DimIPLoc.txt"
    dim_loc_rows = []

    with open(dim_loc_file_path, 'r') as f:
        next(f)  # Skip header row
        for line in f:
            line = line.strip()
            if not line:
                continue

            split_row = line.split(",")

            if len(split_row) == 6:
                lat_value = None
                long_value = None

                if split_row[4] and split_row[4] != "Not found":
                    try:
                        lat_value = float(split_row[4])
                    except ValueError:
                        lat_value = None

                if split_row[5] and split_row[5] != "Not found":
                    try:
                        long_value = float(split_row[5])
                    except ValueError:
                        long_value = None

                dim_loc_rows.append((
                    split_row[0],   # IP
                    split_row[1],   # country_code
                    split_row[2],   # country_name
                    split_row[3],   # city
                    lat_value,      # lat
                    long_value      # long
                ))
            else:
                print("Skipping malformed DimLocationTable row with", len(split_row), "fields:", line)


    # Deduplicate location rows by IP
    unique_dim_loc_rows = {}
    for row in dim_loc_rows:
        ip = row[0]
        if ip not in unique_dim_loc_rows:
            unique_dim_loc_rows[ip] = row

    dim_loc_rows = list(unique_dim_loc_rows.values())

    cur.executemany("""
        INSERT INTO DimLocationTable (
            IP, country_code, country_name, city, lat, long
        )
        VALUES (?, ?, ?, ?, ?, ?)
    """, dim_loc_rows)
    conn.commit()

    # -----------------------------
    # FACT TABLE
    # -----------------------------
    cur.execute("""
        CREATE TABLE FactTable (
            Date TEXT,
            Time TEXT,
            Method TEXT,
            URIStem TEXT,
            IP TEXT,
            UserAgent TEXT,
            Referer TEXT,
            Status INTEGER,
            sc_bytes INTEGER,
            cs_bytes INTEGER,
            TimeTaken INTEGER,
            FOREIGN KEY (Date) REFERENCES DimDateTable(Date),
            FOREIGN KEY (IP) REFERENCES DimLocationTable(IP)
        )
    """)
    conn.commit()

    fact_file_path = StarSchema + "FactTable.txt"
    fact_rows = []

    with open(fact_file_path, 'r') as f:
        next(f)  # Skip header row
        for line in f:
            line = line.strip()
            if not line:
                continue

            split_row = line.split(",")

            if len(split_row) == 11:
                try:
                    fact_rows.append((
                        split_row[0],                                  # Date
                        split_row[1],                                  # Time
                        split_row[2],                                  # Method
                        split_row[3],                                  # URIStem
                        split_row[4],                                  # IP
                        split_row[5],                                  # UserAgent
                        split_row[6],                                  # Referer
                        int(split_row[7]) if split_row[7] else None,   # Status
                        int(split_row[8]) if split_row[8] else None,   # sc_bytes
                        int(split_row[9]) if split_row[9] else None,   # cs_bytes
                        int(split_row[10]) if split_row[10] else None  # TimeTaken
                    ))
                except ValueError:
                    print("Skipping malformed FactTable numeric row:", line)
            else:
                print("Skipping malformed FactTable row with", len(split_row), "fields:", line)

    cur.executemany("""
        INSERT INTO FactTable (
            Date, Time, Method, URIStem, IP, UserAgent, Referer, Status, sc_bytes, cs_bytes, TimeTaken
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, fact_rows)
    conn.commit()

    conn.close()

# ---------------------------------------------------
# DAG DEFINITION
# ---------------------------------------------------
dag = DAG(
   dag_id="Process_Logs_Data",
   schedule="@daily",
   start_date=dt.datetime(2026, 3, 7),
   catchup=False,
)

# TASKS using Python and Bash operators
task_CopyLogFilesToStagingArea = PythonOperator(
   task_id="task_CopyLogFilesToStagingArea",
   python_callable=CopyLogFilesToStagingArea,
   dag=dag,
)

task_BuildFactTable = PythonOperator(
   task_id="task_BuildFactTable",
   python_callable=BuildFactTable,
   dag=dag,
)

task_getIPsFromFactTable = PythonOperator(
    task_id="task_getIPsFromFactTable",
    python_callable=getIPsFromFactTable,
    dag=dag,
)

task_getDatesFromFactTable = PythonOperator(
    task_id="task_getDatesFromFactTable",
    python_callable=getDatesFromFactTable,
    dag=dag,
)

task_makeUniqueIPs = BashOperator(
    task_id="task_makeUniqueIPs",
    bash_command=uniqIPsCommand,
    dag=dag,
)

task_makeUniqueDates = BashOperator(
    task_id="task_makeUniqueDates",
    bash_command=uniqDatesCommand,
    dag=dag,
)

task_makeDateDimension = PythonOperator(
    task_id="task_makeDateDimension",
    python_callable=makeDateDimension,
    dag=dag,
)

task_makeLocationDimension = PythonOperator(
    task_id="task_makeLocationDimension",
    python_callable=makeLocationDimension,
    dag=dag,
)

task_copyFactTable = BashOperator(
    task_id="task_copyFactTable",
    bash_command=copyFactTableCommand,
    dag=dag,
)

task_loadIntoSQLite = PythonOperator(
    task_id="task_loadIntoSQLite",
    python_callable=load_data_to_sqlite,
    dag=dag,
)

# ---------------------------------------------------
# SET UP TASK DEPENDENCIES (Using set_upstream)
# ---------------------------------------------------
task_BuildFactTable.set_upstream(task_CopyLogFilesToStagingArea)
task_getDatesFromFactTable.set_upstream(task_BuildFactTable)
task_getIPsFromFactTable.set_upstream(task_BuildFactTable)
task_makeUniqueDates.set_upstream(task_getDatesFromFactTable)
task_makeUniqueIPs.set_upstream(task_getIPsFromFactTable)
task_makeDateDimension.set_upstream(task_makeUniqueDates)
task_makeLocationDimension.set_upstream(task_makeUniqueIPs)
task_copyFactTable.set_upstream(task_makeDateDimension)
task_copyFactTable.set_upstream(task_makeLocationDimension)
task_loadIntoSQLite.set_upstream(task_copyFactTable)
