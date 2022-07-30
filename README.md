# Edgar Logs Sessionizer

## Tasks
1. Organize the log file and load it into a database
2. Sessionize the log by user. A session lasts about 30 minutes.
3. Analyze the data
   1. Get the top 10 sessions by download count
   2. Get the top 10 sessions by download size

   
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
