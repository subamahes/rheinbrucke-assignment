#!/usr/bin/env python

from rheinbrucke.repository.max_province import *


def service_max_province():
    results,country = select_details()

    result_country = {}
    r =[]

    for i in range(len(country)-1):
        r.append(results[i])
        if country[i] != country[i+1]:
            result_country.__setitem__(country[i],r)
            r=[]
            if len(country) == i+2:
                r.append(results[i+1])
                result_country.__setitem__(country[i+1],r)

    for i in result_country:
        while len(result_country[i]) > 5:
            result_country[i].pop()

    return(result_country)