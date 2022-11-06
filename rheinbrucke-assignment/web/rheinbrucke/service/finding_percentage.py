#!/usr/bin/env python

from rheinbrucke.repository.finding_percentage import *


def service_finding_percentage():
    results , country = select_details()
    percentage_results = select_population_details()

    total_population_2020,total_person_infected = select_total()
    total_percentage_result = dict(
        total_population_2020 = total_population_2020,
        total_person_infected = total_person_infected
    )
    total_percentage_result["percentage_in_decimal"] = round((total_person_infected/total_population_2020),4)
    total_percentage_result["percentage(%)"] = round((total_percentage_result["percentage_in_decimal"]*100),2)


    sum={}

    for i in country: 
        sum[i] = 0

    for result in results:
        sum[result["country"]] = sum[result["country"]] + (result["infected"])

    responses=[]
    for i, val in sum.items():
        for percentage_result in percentage_results:
            if percentage_result["country_name"] == i:
                word = i+"_infected_count"
                percentage_result.__setitem__(word,val)
                if val != 0:
                    percentage_result["percentage_in_decimal"] = round(((percentage_result[word]/percentage_result["population_2020"])),4)
                    percentage_result["percentage(%)"] = round((percentage_result["percentage_in_decimal"]*100),2)
                responses.append(percentage_result)

    resulting_percentage = dict(
        total_world_percentage_infected_2020 = total_percentage_result,
        country_wise_infected_percentage_2020 = responses
    )
    
    return (resulting_percentage)