#!/usr/bin/env python

import json

from rheinbrucke.repository.connection import *


def select_details():
        sql = ("select" 
                " w.population_2020," 
                " c.country," 
                " c.observation_date," 
                " c.confirmed" 
                " from" 
                " rheinbrucke.world_population w" 
                " join" 
                " rheinbrucke.covid_19_data c" 
                " on" 
                " c.country = w.country" 
                " where c.observation_date" 
                " = '2020-12-31';"
               )

        conn = db_connection()
        result = []

        with conn:
            cur = conn.cursor()
            cur.execute(sql)

            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            res={}
            country = set()
            for row in rows:
                res = dict(
                    population_2020 = row[0],
                    country=row[1],
                    date= row[2],
                    infected=row[3]
                )
                country.add(row[1])
                result.append(res)

        cur.close()
        conn.close()
        return result, country

def select_total():
        sql = ("select" 
                " sum(DISTINCT w.population_2020),"
                " sum(c.confirmed)" 
                " from" 
                " rheinbrucke.world_population w" 
                " join" 
                " rheinbrucke.covid_19_data c" 
                " on" 
                " c.country = w.country" 
                " where c.observation_date" 
                " = '2020-12-31';"
               )

        conn = db_connection()
        result = []

        with conn:
            cur = conn.cursor()
            cur.execute(sql)

            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            
            total_population_2020 = rows[0][0]
            total_person_infected = rows[0][1]
        cur.close()
        conn.close()
        return total_population_2020,total_person_infected


def select_population_details():
        sql = ("select distinct" 
                " w.population_2020," 
                " w.country" 
                " from" 
                " rheinbrucke.world_population w" 
                " join" 
                " rheinbrucke.covid_19_data c" 
                " on" 
                " c.country = w.country" 
                " where c.observation_date" 
                " = '2020-12-31';"
               )

        conn = db_connection()
        result = []

        with conn:
            cur = conn.cursor()
            cur.execute(sql)

            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            res={}
            for row in rows:
                res = dict(
                    population_2020 = row[0],
                    country_name = row[1],
                )
                result.append(res)

        cur.close()
        conn.close()
        return result

