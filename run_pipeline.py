import psycopg2
DATABASE_URL = 'postgresql://admin:admin123@localhost:5433/tech_layoffs_dw'
def run_pipeline():
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = True
        cur = conn.cursor()
        with open('pipeline.sql', 'r') as file:
            sql_script = file.read()
        cur.execute(sql_script)
        print('Pipeline executed successfully: Bronze -> Silver -> Gold')
        cur.execute('SELECT SUM("Laid_Off") FROM public.bronze_tech_layoffs;')
        bronze_sum = cur.fetchone()[0]
        cur.execute('SELECT SUM(total_laid_off) FROM gold.fact_layoffs;')
        gold_sum = cur.fetchone()[0]
        print(f'Validation: Bronze={bronze_sum}, Gold={gold_sum}')
        if bronze_sum == gold_sum: print('=> VALIDATION PASSED')
        else: print('=> VALIDATION FAILED')
    except Exception as e: print(f'Error: {e}')
    finally:
        if conn: conn.close()
if __name__ == '__main__': run_pipeline()
