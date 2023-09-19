from multinational_reports import MultiNationalReports
from get_data import GetData

def main():
    print('....................')
    print('Getting the data....')
    GetData.load_and_clean()
    print('All data received and cleaned')
    print('.....................')    
    print('Running the queries...')
    MultiNationalReports.reports()
    print('.....................')
    print('......complete.......')  
    print('.....................')
if __name__ == '__main__':
    print('Running main')
    main()
    

