# EDGAR Logs Sessionizer

I am doing this for a coding challenge. I have decided to do it locally because of the absence of cloud computing resource that I can use personally.

### Goal
   1. Get the top 10 sessions by download count
   2. Get the top 10 sessions by download size

### About the session

Sessions are calculated in 30 minute intervals per user (IP address).
All user activities are accounted for calculating sessions, even in the event of an unsuccessful request (e.g. HTTP 404 Not Found)

### About the source data
- `date` and `time` indicates the time the HTTP request was served to the client. Highly likely in UTC timezone.
- The source data are application logs from Apache HTTP web server. The last segment of the IP address is masked for privacy purposes.
- HTTP requests that are non-erroneous type (e.g. `2xx` ~ `3xx`)
- Requests that are NOT flagged as an index page `idx == 0`. Note: `idx` with the value of `1` is an index page.
- A download URL can be formed by combining `https://www.sec.gov/Archives/edgar/data/` + `cik` (zero padded from the left to form a 10-digit string) + `accession` (hyphen stripped) + `extention`.
  - E.g. `https://www.sec.gov/Archives/edgar/data/0000936340/000095012310088170/c06201e8vk.htm`
- `find` indicates the referrer, in other words, how the user got to that particular URL (document or index page). More about this below in the [Reference](#reference) section.

### Reference
- [Log file directory](https://www.sec.gov/dera/data/edgar-log-file-data-set.html)
- [Field(s) description](https://www.sec.gov/files/EDGAR_variables_FINAL.pdf)
- [Sample file](http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2017/Qtr1/log20170201.zip)

Sample data:
```csv
ip,date,time,zone,cik,accession,extention,code,size,idx,norefer,noagent,find,crawler,browser
107.23.85.jfd,2017-02-01,00:00:00,0.0,1013454.0,0001539497-13-000934,-index.htm,200.0,2792.0,1.0,0.0,0.0,10.0,0.0,
107.23.85.jfd,2017-02-01,00:00:00,0.0,1407200.0,0001193125-16-541564,-index.htm,200.0,2880.0,1.0,0.0,0.0,10.0,0.0,
107.23.85.jfd,2017-02-01,00:00:00,0.0,1013454.0,0001539497-13-000938,-index.htm,200.0,2792.0,1.0,0.0,0.0,10.0,0.0,
107.23.85.jfd,2017-02-01,00:00:00,0.0,1013454.0,0001539497-13-000900,-index.htm,200.0,2791.0,1.0,0.0,0.0,10.0,0.0,
107.23.85.jfd,2017-02-01,00:00:00,0.0,1407200.0,0001193125-16-535603,-index.htm,200.0,2670.0,1.0,0.0,0.0,10.0,0.0,
107.23.85.jfd,2017-02-01,00:00:00,0.0,1013454.0,0001539497-13-000903,-index.htm,200.0,2788.0,1.0,0.0,0.0,10.0,0.0,
107.23.85.jfd,2017-02-01,00:00:00,0.0,1013454.0,0001539497-13-000906,-index.htm,200.0,3406.0,1.0,0.0,0.0,10.0,0.0,
107.23.85.jfd,2017-02-01,00:00:00,0.0,1407200.0,0001407200-16-000127,-index.htm,200.0,2792.0,1.0,0.0,0.0,10.0,0.0,
107.23.85.jfd,2017-02-01,00:00:00,0.0,1013454.0,0001539497-13-000918,-index.htm,200.0,3338.0,1.0,0.0,0.0,10.0,0.0,
```

   
## Directory structure

```                                                                                                                                         git:main*
.
├── LICENSE
├── Pipfile
├── Pipfile.lock
├── ProcessData.ipynb
├── README.md
├── application.log
├── data
│   ├── data.csv
│   └── db.sqlite3
└── sessionizer
    ├── __init__.py
    ├── models.py
    └── sessionizer.py

```
- `ProcessData.ipynb` is the notebook that will orchestrate the whole ETL process to visualization.
- `data` contains the downloaded and extracted log file as well as the SQLite database
- `sessionizer` contains the main logic for sessionizing the log
- `application.log` is where the process logs are going to be recorded
- `Pipfile` and `Pipfile.lock` are Pipenv related files. This will tell pipenv which libraries to install.
- `LICENSE` contains the information regarding the type of license of this project. In our case, it is MIT.

## Requirements

- Python 3

## Installation
1. Install `Pipenv`. `Pipenv` will be the package manager for this project. `python3 -m pip install pipenv`
2. On the project root, run `pipenv install`. This will create a new virtual environment for you and will also install the required libraries.
3. Activate the environment by running `pipenv shell`


## How to execute

Run the notebook `ProcessData.ipynb`.

Note:

If you have an IDE like VSCode or PyCharm, simply use that instead. 
If not, ensure that your virtual environment is activated and then open the notebook via Jupyter command line.

```bash
$ jupyter notebook ProcessData.ipynb
```

## Todo
1. Enhancement: Utilize SQLAlchemy ORM instead of using raw queries
2. Parameterize sessionizer.py during standalone execution
3. Optional: Try to analyze the logs using purely SQL. Chances are, it is going to improve the performance significantly.
