"""Library for interfacing with MST''s MDSplus system."""
import os
import cPickle
import hashlib
import datetime

# This is the MDSplus module that comes with the MDSplus distribution.
import MDSplus as mds

import numpy as np


# Global caches. 
# It turns out that opening and closing connections to the server can 
# cause the server to become unresponsive. This may be because the mdsip 
# processes on the server get locked up some how, I'm not sure. 
_SVR_CONNS = {} # Connection objects indexed by server address.
_SVR_SHOTS = {} # Shot numbers indexed by server address.
_SVR_TREES = {} # Tree names indexed by server address.


def _get_svr_cached(svr):
    """For the given server address, return the cached connection object,
    tree name, and loaded shot number.
    """
    try:
        return _SVR_CONNS[svr], _SVR_TREES[svr], _SVR_SHOTS[svr] 
    except:
        return None, None, None
    
    
def _update_svr_cache(svr, conn, tree, shot):
    _SVR_CONNS[svr] = conn
    _SVR_TREES[svr] = tree
    _SVR_SHOTS[svr] = shot



def get_server_for_shot(shot):
    """Get the server address for the given shot number. Shots for the 
    current day are in aurora, while previous days are on dave.
    """
    first = min_shot_for_date(datetime.date.today())
    if shot >= first:
        return 'aurora.physics.wisc.edu'
    else:
        return 'dave.physics.wisc.edu'


def get_connection(shot, tree='mst'):
    """Get an MDSplus connection object connected to the appropriate 
    server and having the given tree and shot opened. 
    """
    svr = get_server_for_shot(shot)
    conn, tree_, shot_ = _get_svr_cached(svr)

    if conn is None:
        conn = mds.Connection(svr)
        
    if tree != tree_ or shot != shot_:
        try:
            # This throws an exception if there are no open trees. 
            conn.closeAllTrees()
        except:
            pass
        conn.openTree(tree, shot)
        
    _update_svr_cache(svr, conn, tree, shot)

    return conn


def _cache_path(tree, shot, expr):
    """Get the cache path for the given tree, shot, and expression."""
    dir_ = '~/.mdsplus_cache/{0}/{1}/'.format(tree, shot)
    dir_ = os.path.expanduser(dir_)
    fn = hashlib.md5(expr).hexdigest()
    return dir_, fn


def _save_to_cache(tree, shot, expr, data):
    """Save the data to cache."""
    dir_, fn = _cache_path(tree, shot, expr)
    if not os.path.exists(dir_):
        os.makedirs(dir_)
        
    path = os.path.join(dir_, fn)

    with open(path, 'wb') as f:
        cPickle.dump(data, f, protocol=-1)
    
    
def _load_from_cache(tree, shot, expr):
    """Load the cached data."""
    dir_, fn = _cache_path(tree, shot, expr)
    path = os.path.join(dir_, fn)
    with open(path, 'rb') as f:
        return cPickle.load(f)


def get_signal(shot, signal, tree='mst', use_cache=True):
    """Get a signal from the MDSplus system. 
    Arguments:
    shot      -- The shot number.
    signal    -- The signal name.
    tree      -- The tree. Defaults to 'mst'.
    use_cache -- If True, try to read the signal from cache. 
    """
    # This is the mdsplus expression we're going to try to execute.
    expr = '[if_error(dim_of({0}), $roprand), if_error({0}, $roprand)]'.format(
        signal)


    if use_cache:
        try:
            return _load_from_cache(tree, shot, expr)
        except:
            pass
        
    conn = get_connection(shot, tree)

    t, y = conn.get(expr).data()

    assert(isinstance(t, np.ndarray))
    assert(isinstance(y, np.ndarray))
    
    if use_cache:
        _save_to_cache(tree, shot, expr, (t, y))
    
    return t, y


def get_signal_units(shot, signal, tree='mst', use_cache=True):
    """Get the units associated with the given signal."""
    expr = 'units({0})'.format(signal)
    
    if use_cache:
        try:
            return _load_from_cache(tree, shot, expr)
        except:
            pass
        
    conn = get_connection(shot, tree)

    units = conn.get(expr.format(signal)).data().tostring()
    
    if use_cache:
        _save_to_cache(tree, shot, expr, units)

    return units



def min_shot_for_date(date):
    """Return the minimum shot number for the given date."""
    y = date.year
    m = date.month
    d = date.day
    return (y % 1000 + 100) * 10000000 + m * 100000 + d * 1000 + 1



def max_shot_for_date(date):
    """Return the maximum shot for the given date."""
    return min_shot_for_date(date) + 998



def shot_to_date(shot):
    """Return the date corresponding to the given shot number."""
    day = int(shot / 1000) % 100
    month = int(shot / 100000) % 100
    year = int(shot / 10000000) + 1900
    return datetime.date(year, month, day)



def shot_to_date_num(shot):
    """Given a shot, return a number corresponding to the full date. 
    For example, given 1100502001, return 20100502. 
    """
    return date_to_date_num(shot_to_date(shot))



def date_to_date_num(date):
    """For a given date, return the canonical date number. For example, 
    Jan 12, 2011 will be 20110112.
    """
    return int(date.strftime('%Y%m%d'))



def shot_valid(shot):
    """Return true if the shot number is valid."""
    if shot > 9991212999 or shot < 1000101001 or shot % 1000 == 0:
        return False
    try:
        shot_to_date(shot)
        return True
    except Exception:
        return False



def current_shot():
    global _AURORA
    try:
        return int(_AURORA.get('current_shot("mst")'))
    except:
        _AURORA = mds.Connection('aurora.physics.wisc.edu')
        return int(_AURORA.get('current_shot("mst")'))
