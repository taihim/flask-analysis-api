from pathlib import Path
import subprocess
import os

def create_db(source_name='dataset.csv', database_name='dataset.db', table_name='sample_data'):
    
    csv_file = Path('data/{}'.format(source_name)).resolve()
    db_name = Path('db/{}'.format(database_name)).resolve()
    
    subprocess.run(['sqlite3',
                    str(db_name),
                    '-cmd',
                    '.mode csv',
                    '.import ' + str(csv_file) +' {}'.format(table_name)]
                    )

create_db()
