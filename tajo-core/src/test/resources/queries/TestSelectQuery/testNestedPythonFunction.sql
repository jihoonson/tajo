select * from nation where sum_py(n_regionkey, return_one()) < 2