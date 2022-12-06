from configparser import ConfigParser

def config(filename='files/database.ini', section='postgresql'):
    
    parser =  ConfigParser()
    
    #Read config data from file
    parser.read(filename)

    #dict for DB parameters
    db={}

    #Checks for section in .ini file
    if parser.has_section(section):
        
        #Parses the configurations from the section in .ini file
        params = parser.items(section)

        #Updates DB dict with parameters
        for param in params:
            db[param[0]] = param[1]

    else:
        raise KeyError(f'Section {section} is not found in the {filename}')

    return db

    
    
    

