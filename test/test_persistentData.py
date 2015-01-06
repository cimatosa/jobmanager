import os
import sys

sys.path.append(os.path.dirname(__file__))

import persistentData as pd

def test_pd():
    with pd.PersistentDataStructure(name='test_data') as data:
        key = 'a'
        value = 1
        data.setData(key=key, value=value)
        assert data.getData(key) == value
        
        
        key_sub = 'zz'
        with data.getData(key_sub, create_sub_data=True) as sub_data:
            sub_data.setData(key=key, value=3)
            assert sub_data.getData(key) == 3
            assert data.getData(key) == 1
            

            with sub_data.getData(key_sub, create_sub_data=True) as sub_sub_data:
                sub_sub_data.setData(key=key, value=4)
                assert sub_sub_data.getData(key) == 4
                assert sub_data.getData(key) == 3
                assert data.getData(key) == 1

    
    
    
if __name__ == "__main__":
    test_pd()
    
