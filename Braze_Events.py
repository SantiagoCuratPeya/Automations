### IMPORTS
import sys, os
import pathlib
sys.path.append(str(pathlib.Path().resolve())+'\\Generals')

from Generals import *
from CRM_Central import *
from Braze import *

# Credenciales
cred_sheets = get_credentials('Sheets')
cred_bq = get_credentials('BQ')

### TRABAJO

# Consigo las credenciales de Braze
braze = download('1d4cMi-V04c80Dpen4S70Q7zYqqho3ZBIVctlRIBLwiw', 'Braze', cred_sheets)
url = braze[braze['Type'] == 'url']['Key'].values[0]
api = braze[braze['Type'] == 'api']['Key'].values[0]

# Queries
q = '''WITH trusted_table AS (
      SELECT cm.user_id AS user_id,
             MAX(cm.date_partition) AS fecha
      FROM `peya-fraud-and-fintech.group_pf_operativo.pf_customer_metrics` AS cm
      WHERE cm.date_partition >= DATE_ADD(CURRENT_DATE(),INTERVAL -7 DAY)
      GROUP BY 1)
SELECT o.order_id AS order_id,
       o.user.id AS user_id,
       IFNULL(o.total_amount_with_delivery_costs,0) AS total_amount_local,
       o.registered_date AS fecha,
       o.registered_at AS fecha_at,
       o.fail_rate_owner AS fr_owner,
       cn.country_name AS country,
       ul.lifecycle_stage AS lifecycle_stage,
       ul.health_status AS health_status,
       ts.trust_segment AS trust_segment,
       ts.date_partition AS fecha_trust,
       TIME_DIFF(TIME(MAX(osc.response_date)),TIME(o.registered_at),MINUTE) AS mins_to_cancellation,
       CASE WHEN COUNT(DISTINCT oc.order_id) > 0 THEN TRUE ELSE FALSE END AS is_comp
FROM `peya-bi-tools-pro.il_core.fact_orders` AS o
INNER JOIN `peya-bi-tools-pro.il_core.dim_country` AS cn ON o.country_id = cn.country_id AND cn.active
LEFT JOIN `peya-bi-tools-pro.il_growth.agg_user_lifecycle` AS ul ON o.user.id = ul.user_id
LEFT JOIN trusted_table AS tt ON o.user.id = tt.user_id
LEFT JOIN `peya-fraud-and-fintech.group_pf_operativo.pf_customer_metrics` AS ts ON o.user.id = ts.user_id AND tt.fecha = ts.date_partition AND ts.date_partition >= DATE_ADD(CURRENT_DATE(),INTERVAL -7 DAY)
LEFT JOIN `peya-data-origins-pro.cl_compensations.user_compensations` AS oc ON o.order_id = oc.order_id AND DATE(oc.startdate) >= DATE_ADD(CURRENT_DATE(),INTERVAL -2 DAY)
LEFT JOIN `peya-bi-tools-pro.il_core.fact_orders_status_changes` AS osc ON o.order_id = osc.order_id AND osc.order_status = "REJECTED" AND DATE(osc.response_date) >= DATE_ADD(CURRENT_DATE(),INTERVAL -2 DAY) 
WHERE o.order_status = 'REJECTED'
      AND o.registered_date = DATE_ADD(CURRENT_DATE(),INTERVAL -1 DAY)
      AND (ts.trust_segment != 'Untrusted' OR ts.trust_segment IS NULL)
      AND (ul.health_status != 'Unhealthy' OR ul.health_status IS NULL)
      AND cn.country_name NOT IN ('Bolivia','Uruguay','República Dominicana','Panamá')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
HAVING fr_owner != 'User'
       AND mins_to_cancellation BETWEEN 2 AND 180
       AND is_comp = FALSE
ORDER BY mins_to_cancellation DESC,1,2'''

# Descargo la data
#bq = pd.io.gbq.read_gbq(q, credentials=cred_bq, project_id='peya-growth-and-onboarding', dialect='standard')

# Copio las bases
#df = bq.copy()

df = pd.DataFrame.from_dict({'user_id': [3137104,1900147], 'amount_paid_by_user': [250,100]})

### CARGA

# Llamo a la funcion de propiedades
prop = create_prop(df, 'user_id')
# Llamo a la funcion batch_users
users = batch_users(df['user_id'].tolist(), "autocomp_cancellation", prop)
# Hago el upload
res = upload_braze(users, api, url)

### LOG

# Descargo el File de Logs
log_br = download('1FexQYU7YobobnchL45JD0nReY6gPUD0WSZktkLrwEck', 'Log', cred_sheets)
# Creo la fila para el Append
now = datetime.utcnow() - timedelta(hours=3)
row = pd.DataFrame({'Fecha':[now.date().strftime('%Y-%m-%d')], 'Hora Argentina':[now.time().strftime('%H:%M:%S')], 'Ordenes':[len(df['user_id'])], 'Resultado':[res]})
# Hago append de la nueva fila al Log
log_br = pd.concat([log_br, row], sort=False)
# Carga de Log Braze
log = load(log_br, '1FexQYU7YobobnchL45JD0nReY6gPUD0WSZktkLrwEck', 'Log', cred_sheets, 'Log Braze', log)
# Cargo el Log Final
load(log, '10VJBek_8UvxOiEoY4u7tICk2pLJZ9HZ6zfOq81GY20s', 'Logs_Braze', cred_sheets)