#resource used: https://docs.dask.org/en/stable/delayed.html
#resource used: https://docs.dask.org/en/stable/futures.html

from dask import delayed
from hwfunctions import fun_factor, fun_inc

NUM_WORKERS = 4
#create parallel version of serial_inc
#serial_inc function calls fun_inc
#fun_inc takes an integer as input and returns the integer incremented

#serial_inc function returns ([fun_inc(i) for i in range(start, end)]) 
#where start and end are the arguments passed into serial_inc

#delayed_increment does the same thing as serial_inc but uses dask delayed

#returns delayed object
def delayed_increment(c, start, end):
    if (end-start)/NUM_WORKERS < 1_000_000:
        z = delayed(serial_inc)(start, end)
        return z
    else:
        batch_size = int((end-start)/NUM_WORKERS)
        sims = [serial_inc(start + i*batch_size, end) for i in range(NUM_WORKERS)]
        tot_delayed = delayed(sum)(sims)
        return tot_delayed
        #return delayed_increment_result
        # use fun_inc
    
#returns delayed object
def delayed_factor(c, start, end):
    if start < 5000 and (end-start)/NUM_WORKERS < 100:
        z = delayed(serial_factor(start, end))
        return z
    elif start > 5000 and (end-start)/NUM_WORKERS < 2:
        z = delayed(serial_factor(start, end))
        return z
    elif start > 5000 and (end-start)/NUM_WORKERS > 2:
        batch_size = int((end-start)/NUM_WORKERS)
        sims = [delayed(serial_factor)(start + i*batch_size, end) for i in range(NUM_WORKERS)]
        tot_delayed = delayed(sum)(sims)
        return tot_delayed
    # use fun_factor
    

#returns future object
def future_increment(c, start, end):
    if (end-start)/NUM_WORKERS < 1_000_000:
        x = c.map(fun_inc, range(start, end))
        sum_of_increments = c.submit(sum, x)
        return sum_of_increments
    else:
        batch_size = int((end-start)/NUM_WORKERS)
        sims = [serial_inc(start + i*batch_size, end) for i in range(NUM_WORKERS)]
        tot_future = c.submit(sum, sims)
        return tot_future

#returns future object
def future_factor(c, start, end):
    if start < 5000 and (end-start)/NUM_WORKERS < 100:
        x = c.map(fun_factor, range(start, end))
        sum_of_factors = c.submit(sum, x)
        return sum_of_factors
    elif start > 5000 and (end-start)/NUM_WORKERS < 2:
        x = c.map(fun_factor, range(start, end))
        sum_of_factors = c.submit(sum, x)
        return sum_of_factors
    elif start > 5000 and (end-start)/NUM_WORKERS > 2:
        batch_size = int((end-start)/NUM_WORKERS)
        for i in NUM_WORKERS:
            sims = c.map(serial_factor, (start + i*batch_size, end))
        tot_future = c.submit(sum, sims)
        return tot_future
    # use fun_factor
    


   #serial functions
def serial_inc(start, end):
    z =  sum([fun_inc(i) for i in range(start, end)])
    return z

def serial_factor(start, end):
    z = sum([fun_factor(i) for i in range(start, end)])
    return z
