from collections import defaultdict
import glob
import traceback
import re


def read_write_map(file_name: str, offset: int, limit: int, m:int) -> None:
    '''
        This function uses pointers to get to a specific point in the file where the meta data specifies it should work
        on.

        Returns None

        Parameters
        ----------
        file_name: the name of the file to process
        offset: point where to seek from
        limit: end of file partition allocated by meta data
        m: number of possible reduces
        is_last: boolean if it is the last map task scheduled by master

        Returns
        --------
        None
    '''
    try:  
        f = open(file_name, "r")
        f.seek(offset * limit)
        data = f.read(limit)
        line_by_line = data.split()

        for text in line_by_line:
            text = re.sub(r'[^a-zA-Z]', '', text)
            if len(text) == 0:
                continue
            index = ord(text[0]) % m
            file_name = f"intermediate/mr-{offset}-{index}"
            try:
                fo = open(file_name, "a+")
                fo.write(f"{text}\n")
            except:
                error = traceback.format_exc()
                print(error)
            finally:        
                fo.close()
    except:
        error = traceback.format_exc()
    finally:
        f.close()   


def read_write_reduce(index: int) -> None:
    '''
        This function gets all the files with a certain reduce number and does the word count.

        Returns None

        Parameters
        ----------
        index: the reduce number

        Returns
        --------
        None
    '''
    count = defaultdict(int)
    for name in glob.glob(f"intermediate/*[{index}]"):
        try:
            f = open(name, "r")
            data = f.read()
            data = data.split()
            
            for text in data:
                count[text] +=1
        except:
            error = traceback.format_exc()
            print(error)
        finally:
            f.close()
    file_name = f"out/out-{index}"
    try:
        fo = open(file_name, "a+")
    
        for k,v in count.items():
            data = f"{k} {v} \n" 
            fo.write(data)
    except:
        error = traceback.format_exc()
        print(error)
    finally:
        fo.close()