import pytest
import os
import sys
import glob
sys.path.append(os.path.dirname(os.getcwd()))
from worker_map import read_write_map
from worker_reduce import read_write_reduce



def test_read_write_map():
    read_write_map(f"inputs/my_test.txt", 0, 72, 8, True)
    
    f = open("intermediate/mr-0-4")
    line = f.readline()
    line = line.split()
    assert line[0] == "This"
    f.close()


def test_read_write_reduce():
    read_write_reduce(4)
    
    f = open("out/out-4")
    line = f.readline()
    line = line.split()
    assert int(line[1]) == 3
    f.close()
    
    # clean up
    for name in glob.glob(f"intermediate/*"):
        os.remove(name)
        
    for name in glob.glob(f"out/*"):
        os.remove(name)




