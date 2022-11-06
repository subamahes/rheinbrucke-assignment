#!/usr/bin/env python

import json

from rheinbrucke.repository.connection import *


def select_details():
        sql = ("SELECT"
                " c.country," 
                " c.province," 
                " c.confirmed" 
                " from" 
                " rheinbrucke.covid_19_data c"  
                " where c.observation_date" 
                " = '2020-12-31'"
                " order by c.country ASC ,"
                " c.confirmed DESC;"
               )

        conn = db_connection()
        result = []

        with conn:
            cur = conn.cursor()
            cur.execute(sql)

            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            country = []
            for row in rows:
                res = dict(
                    country = row[0],
                    province=row[1],
                    confirmed= row[2]
                )
                country.append(row[0])
                result.append(res)
            country.sort()
        cur.close()
        conn.close()
        return result,country