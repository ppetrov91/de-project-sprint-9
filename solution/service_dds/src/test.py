import psycopg
import os

'''
dds_loader/sql/fill_h_order.sql"
'''

ids = [4426665,4426666,4426667,4426668,4426669,4426670,4426671,4426672,4426673,4426674,4426675,4426676,4426677,4426678,4426679,4426680,4426681,4426682,4426683,4426684,4426685,4426686,4426687,4426688,4426689,4426690,4426691,4426692,4426693,4426694,4426695,4426696,4426697,4426698,4426699,4426700,4426701,4426702,4426703,4426704,4426705,4426706,4426707,4426708,4426709,4426710,4426711,4426712,4426713,4426714,4426715,4426716,4426717,4426718,4426719,4426720,4426721,4426722,4426723,4426724,4426725,4426726,4426727,4426728,4426729,4426730,4426731,4426732,4426733,4426734,4426735,4426736,4426737,4426738,4426739,4426740,4426741,4426742,4426743,4426744,4426745,4426746,4426747,4426748,4426749,4426750,4426751,4426752,4426753,4426754,4426755,4426756,4426757,4426758,4426759,4426986,4426987,4426988,4426989,4426990,4426991,4426992,4426993,4426994,4426995,4426996,4426997,4426998,4426999,4427000,4427001,4427002,4427003,4427004,4427005,4427006,4427007,4427008,4427009,4427010,4427011,4427012,4427013,4427014,4427015,4427016,4427017,4427018,4427019,4427020,4427021,4427022,4427023,4427024,4427025,4427026,4427027,4427028,4427029,4427030,4427031,4427032,4427033,4427034,4427035,4427036,4427037,4427038,4427039,4427040,4427041,4427042,4427043,4427044,4427045,4427046,4427047,4427048,4427049,4427050,4427051,4427052,4427053,4427054,4427055,4427056,4427057,4427058,4427059,4427060,4427061,4427062,4427063,4427064,4427065,4427066,4427067,4427068,4427069,4427070,4427071,4427072,4427073,4427074,4427075,4427076,4427077,4427078,4427079,4427080,4427081,4427082,4427083,4427084,4427085,4427273,4427274,4427275,4427276]


def execute_sql_script(filepath, params, cur):
    with open(filepath, "r") as f:
        sql = f.read()
        cur.execute(sql, params)

with psycopg.connect("dbname=dwh user=postgres") as conn:
    with conn.transaction() as trans, conn.cursor() as cur:
        sql_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                "dds_loader/sql/fill_s_order_status.sql")


        #execute_sql_script(sql_path, 
        #                   ["stg-service-orders", ids, "order"], 
        #                   cur)

        execute_sql_script(sql_path, 
                           [ids, "order", "stg-service-orders"], 
                           cur)