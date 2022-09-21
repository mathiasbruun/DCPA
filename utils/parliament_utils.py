import shutil
import urllib.request as request
from contextlib import closing
from urllib.error import URLError

def save_xml_from_ftp(url: 'str', out_path: 'str (.xml/.txt)') -> 'XML file @ out_path':
    '''
    TODO...
    '''
    
    try:
        with closing(request.urlopen(url)) as r:
            with open(out_path, 'wb') as f:
                shutil.copyfileobj(r, f)

    except URLError as e:
        if e.reason.find('No such file or directory') >= 0:
            raise Exception('FileNotFound')
        else:
            raise Exception(f'Something else happened. "{e.reason}"')