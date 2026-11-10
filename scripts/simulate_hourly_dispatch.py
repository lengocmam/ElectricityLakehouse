import urllib.request, json, sys, os
sys.path.insert(0, os.path.dirname(__file__))

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

from check_energy_balance import run_sql

calibrated_profile = [
    ( 0, 1.0025, 0.6016, 0.9243, 0.0000, 1.0615, 1.0000),
    ( 1, 0.9825, 0.5469, 0.9028, 0.0000, 1.0918, 1.0000),
    ( 2, 0.9724, 0.5251, 0.8813, 0.0000, 1.1120, 1.0000),
    ( 3, 0.9724, 0.5251, 0.8706, 0.0000, 1.1120, 1.0000),
    ( 4, 0.9825, 0.5469, 0.8813, 0.0000, 1.0918, 1.0000),
    ( 5, 1.0025, 0.6563, 0.9136, 0.0000, 1.0312, 1.0000),
    ( 6, 1.0125, 0.8751, 0.9673, 0.1193, 0.9604, 1.0000),
    ( 7, 1.0226, 1.1486, 1.0210, 0.5963, 0.9099, 1.0000),
    ( 8, 1.0326, 1.2580, 1.0533, 1.3416, 0.8593, 1.0000),
    ( 9, 1.0025, 1.1486, 0.9673, 2.1863, 0.8290, 1.0000),
    (10, 0.9323, 0.9845, 0.8598, 2.9317, 0.8088, 1.0000),
    (11, 0.8622, 0.8204, 0.7846, 3.2298, 0.7885, 1.0000),
    (12, 0.8220, 0.7439, 0.7525, 3.3292, 0.7887, 1.0000),
    (13, 0.8521, 0.7876, 0.7631, 3.1304, 0.8088, 1.0000),
    (14, 0.9223, 0.9298, 0.8276, 2.7826, 0.8492, 1.0000),
    (15, 1.0025, 1.1486, 0.9673, 2.1366, 0.9099, 1.0000),
    (16, 1.0526, 1.3674, 1.1178, 1.4410, 0.9705, 1.0000),
    (17, 1.1028, 1.5861, 1.2682, 0.6758, 1.0716, 1.0000),
    (18, 1.1429, 1.6955, 1.3757, 0.0994, 1.1929, 1.0000),
    (19, 1.1228, 1.6190, 1.3435, 0.0000, 1.2131, 1.0000),
    (20, 1.0927, 1.4768, 1.2897, 0.0000, 1.1929, 1.0000),
    (21, 1.0627, 1.2580, 1.2038, 0.0000, 1.1626, 1.0000),
    (22, 1.0326, 0.9845, 1.0748, 0.0000, 1.1120, 1.0000),
    (23, 1.0125, 0.7657, 0.9888, 0.0000, 1.0716, 1.0000),
]

def simulate(target_date):
    print(f"\n=========================================================================================")
    print(f"HOURLY BALANCE SIMULATION: {target_date}")
    print(f"=========================================================================================")
    _, load_rows = run_sql(f"SELECT hour_of_day, load_mw FROM gold.v_load_national_hourly WHERE date_value = DATE '{target_date}' ORDER BY 1")
    load = {r[0]: r[1] for r in load_rows}

    q_gen = f"""
    SELECT 
        CASE 
            WHEN LOWER(energy_source) LIKE '%than%' THEN 'Than'
            WHEN LOWER(energy_source) LIKE '%khí%' OR LOWER(energy_source) LIKE '%gas%' THEN 'Khí'
            WHEN LOWER(energy_source) LIKE '%thủy điện%' THEN 'Thủy điện'
            WHEN LOWER(energy_source) LIKE '%mặt trời%' OR LOWER(energy_source) LIKE '%đmt%' THEN 'Mặt trời'
            WHEN LOWER(energy_source) LIKE '%gió%' THEN 'Gió'
            ELSE 'Khác'
        END AS source_group,
        SUM(generation_million_kwh * 1000.0) AS generation_mwh
    FROM iceberg.silver.generation
    WHERE data_date = DATE '{target_date}' AND energy_source NOT LIKE '%thương phẩm%'
    GROUP BY 1
    """
    _, gen_rows = run_sql(q_gen)
    gen = {r[0]: r[1] for r in gen_rows}
    
    print(f"{'Hour':>4} | {'Load':>8} | {'Coal':>8} | {'Gas':>7} | {'Hydro':>8} | {'Solar':>8} | {'Wind':>7} | {'Other':>7} | {'TotalGen':>8} | {'Diff':>7} | {'Diff%':>6}")
    print("-" * 105)
    
    for row in calibrated_profile:
        h, cf, gf, hf, sf, wf, of = row
        c_mw = (gen.get('Than', 0) / 24.0) * cf
        g_mw = (gen.get('Khí', 0) / 24.0) * gf
        hy_mw = (gen.get('Thủy điện', 0) / 24.0) * hf
        s_mw = (gen.get('Mặt trời', 0) / 24.0) * sf
        w_mw = (gen.get('Gió', 0) / 24.0) * wf
        o_mw = (gen.get('Khác', 0) / 24.0) * of
        tot_gen = c_mw + g_mw + hy_mw + s_mw + w_mw + o_mw
        l_mw = load.get(h, tot_gen)
        diff = l_mw - tot_gen
        diff_pct = (diff / l_mw) * 100.0
        print(f"{h:4d} | {l_mw:8.0f} | {c_mw:8.0f} | {g_mw:7.0f} | {hy_mw:8.0f} | {s_mw:8.0f} | {w_mw:7.0f} | {o_mw:7.0f} | {tot_gen:8.0f} | {diff:7.0f} | {diff_pct:5.1f}%")

simulate('2026-09-16')
simulate('2026-10-10')
