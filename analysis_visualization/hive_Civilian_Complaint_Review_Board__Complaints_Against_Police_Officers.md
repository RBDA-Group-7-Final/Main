# Initialize Hive

```bash
#!/bin/bash
beeline -u jdbc:hive2://localhost:10000
set hive.execution.engine=mr;
set hive.fetch.task.conversion=minimal;
```

# Create Table
```sql
use rm5327_nyu_edu;

create external table complaints (
    incident_time STRING,
    receive_time STRING,
    close_time STRING,
    borough STRING,
    location_type STRING,
    contact_reason STRING,
    outcome STRING,
    dispotion STRING)
    row format delimited
    fields terminated by ','
    location '/user/rm5327_nyu_edu/output/';
```

# Verify Creation
```sql
show tables;
describe formatted complaints;

SELECT * FROM complaints LIMIT 10;
```
```bash
+---------------------------+--------------------------+-------------------------+---------------------+---------------------------+-----------------------------------------------+-----------------------------------+-------------------------------------+
| complaints.incident_time  | complaints.receive_time  |  complaints.close_time  | complaints.borough  | complaints.location_type  |           complaints.contact_reason           |        complaints.outcome         |        complaints.dispotion         |
+---------------------------+--------------------------+-------------------------+---------------------+---------------------------+-----------------------------------------------+-----------------------------------+-------------------------------------+
| 01/01/2000 5              | 01/03/2000 12:00:00 AM   | 05/26/2000 10:37:39 AM  | QUEENS              | Street/highway            | PD suspected C/V of violation/crime - street  | UNKNOWN                           | Substantiated (Command Discipline)  |
| 01/01/2000 3              | 01/03/2000 12:00:00 AM   | 09/28/2000 05:14:26 PM  | BROOKLYN            | UNKNOWN                   | Report-domestic dispute                       | No arrest made or summons issued  | Unfounded                           |
| 01/01/2000 1              | 01/03/2000 12:00:00 AM   | 07/20/2001 08:40:55 AM  | BROOKLYN            | Street/highway            | Other                                         | Arrest - other violation/crime    | Unfounded                           |
| 01/02/2000 16             | 01/03/2000 12:00:00 AM   | 01/25/2001 03:04:55 PM  | BRONX               | Street/highway            | Report-dispute                                | Arrest - other violation/crime    | Unsubstantiated                     |
| 01/03/2000 13             | 01/03/2000 12:00:00 AM   | 11/14/2000 03:51:47 PM  | MANHATTAN           | UNKNOWN                   | C/V telephoned PCT                            | No arrest made or summons issued  | Exonerated                          |
| 01/04/2000 8              | 01/04/2000 12:00:00 AM   | 12/12/2000 05:37:17 PM  | BRONX               | Apartment/house           | Other                                         | No arrest made or summons issued  | Substantiated (Command Discipline)  |
| 01/03/2000 16             | 01/04/2000 12:00:00 AM   | 03/30/2001 11:35:37 AM  | BROOKLYN            | Street/highway            | Other                                         | Arrest - other violation/crime    | Unsubstantiated                     |
| 01/02/2000 0              | 01/04/2000 12:00:00 AM   | 05/31/2000 09:17:55 AM  | STATEN ISLAND       | UNKNOWN                   | Other                                         | No arrest made or summons issued  | Complaint Withdrawn                 |
| 01/03/2000 18             | 01/04/2000 12:00:00 AM   | 12/22/2001 11:36:33 AM  | MANHATTAN           | Residential building      | PD suspected C/V of violation/crime - bldg    | No arrest made or summons issued  | Substantiated (Charges)             |
| 10/02/1999 16             | 01/04/2000 12:00:00 AM   | 09/28/2000 05:23:13 PM  | BRONX               | Street/highway            | PD suspected C/V of violation/crime - street  | No arrest made or summons issued  | Unfounded                           |
+---------------------------+--------------------------+-------------------------+---------------------+---------------------------+-----------------------------------------------+-----------------------------------+-------------------------------------+
```

# Get Total Record Count
```sql
SELECT COUNT(*) AS total_records FROM complaints;
```
```bash
+----------------+
| total_records  |
+----------------+
| 125266         |
+----------------+
```

# Distribution of Records by Borough
```sql
SELECT borough, COUNT(*) AS count
FROM complaints
GROUP BY borough
ORDER BY count DESC;
```
```bash
+----------------+--------+
|    borough     | count  |
+----------------+--------+
| BROOKLYN       | 41712  |
| MANHATTAN      | 30394  |
| BRONX          | 27262  |
| QUEENS         | 20093  |
| STATEN ISLAND  | 5226   |
| UNKNOWN        | 451    |
| OUTSIDE NYC    | 128    |
+----------------+--------+
```

# Distribution by Location Type
```sql
SELECT location_type, COUNT(*) AS count
FROM complaints
GROUP BY location_type
ORDER BY count DESC;
```
```bash
+------------------------+--------+
|     location_type      | count  |
+------------------------+--------+
| Street/highway         | 67351  |
| Apartment/house        | 22116  |
| Police building        | 8165   |
| Residential building   | 6267   |
| Subway station/train   | 5318   |
| Commercial building    | 4052   |
| UNKNOWN                | 2702   |
| Public space/building  | 2299   |
| Other                  | 2247   |
| NYCHA                  | 1632   |
| Park                   | 1415   |
| Hospital               | 797    |
| School                 | 347    |
| Police vehicle         | 192    |
| Phone                  | 169    |
| Bus                    | 120    |
| River or waterway      | 77     |
+------------------------+--------+
```

# Distribution of Outcomes
```sql
SELECT outcome, COUNT(*) AS count
FROM complaints
GROUP BY outcome
ORDER BY count DESC;
```
```bash
+---------------------------------------+--------+
|                outcome                | count  |
+---------------------------------------+--------+
| No arrest made or summons issued      | 61628  |
| Arrest - other violation/crime        | 33323  |
| Summons - other violation/crime       | 6872   |
| Summons - disorderly conduct          | 4459   |
| Moving violation summons issued       | 4102   |
| Arrest - resisting arrest             | 3696   |
| Arrest - disorderly conduct           | 2532   |
| Other VTL violation summons issued    | 2290   |
| Parking summons issued                | 2053   |
| Arrest - assault (against a PO)       | 2023   |
| Arrest - OGA                          | 1388   |
| UNKNOWN                               | 616    |
| Juvenile Report                       | 185    |
| Arrest - harrassment (against a PO)   | 62     |
| Summons - OGA                         | 22     |
| Summons - harrassment (against a PO)  | 15     |
+---------------------------------------+--------+
```

# Distribution of Dispotions
```sql
SELECT dispotion, COUNT(*) AS count
FROM complaints
GROUP BY dispotion
ORDER BY count DESC;
```
```bash
+-------------------------------------------+--------+
|                 dispotion                 | count  |
+-------------------------------------------+--------+
| Complainant Uncooperative                 | 34159  |
| Unsubstantiated                           | 18737  |
| Complaint Withdrawn                       | 14109  |
| Complainant Unavailable                   | 11372  |
| Unfounded                                 | 7877   |
| Exonerated                                | 7767   |
| Alleged Victim Uncooperative              | 4860   |
| Substantiated (Charges)                   | 3764   |
| Mediation Attempted                       | 3284   |
| Mediated                                  | 2942   |
| Officer(s) Unidentified                   | 2812   |
| Closed - Pending Litigation               | 2627   |
| Alleged Victim Unavailable                | 2086   |
| Substantiated (Command Discipline A)      | 1306   |
| Unable to Determine                       | 1122   |
| Substantiated (Command Discipline B)      | 921    |
| OMB PEG Directive Closure                 | 913    |
| Substantiated (Formalized Training)       | 802    |
| Within NYPD Guidelines                    | 751    |
| Substantiated (Command Discipline)        | 719    |
| Victim Unidentified                       | 628    |
| Administratively Closed                   | 481    |
| Substantiated (Command Lvl Instructions)  | 303    |
| Miscellaneous - Subject Retired           | 282    |
| Substantiated (Instructions)              | 256    |
| Miscellaneous                             | 152    |
| Miscellaneous - Subject Resigned          | 143    |
| Substantiated (No Recommendations)        | 49     |
| Witness Uncooperative                     | 20     |
| Miscellaneous - Subject Terminated        | 14     |
| Witness Unavailable                       | 5      |
| SRAD Closure                              | 3      |
+-------------------------------------------+--------+
```
[Dispotions](https://www.nyc.gov/site/ccrb/investigations/case-outcomes.page)

# Count of Records Grouped by Date
```sql
SELECT
    SUBSTR(incident_time, 1, 10) AS incident_date,
    COUNT(*) AS incident_count
FROM complaints
GROUP BY SUBSTR(incident_time, 1, 10)
ORDER BY incident_count DESC
LIMIT 10;
```
```bash
+----------------+-----------------+
| incident_date  | incident_count  |
+----------------+-----------------+
| 05/30/2020     | 81              |
| 06/04/2020     | 63              |
| 02/15/2003     | 60              |
| 09/04/2006     | 58              |
| 05/29/2020     | 50              |
| 06/02/2020     | 49              |
| 08/31/2004     | 45              |
| 06/03/2020     | 42              |
| 04/07/2006     | 41              |
| 10/31/2006     | 41              |
+----------------+-----------------+
```

# Count of Records Grouped by Month
```sql
SELECT
    SUBSTR(incident_time, 1, 2) AS month,
    COUNT(*) AS incident_count
FROM complaints
GROUP BY SUBSTR(incident_time, 1, 2)
ORDER BY incident_count DESC;
```
```bash
+-- -----+-----------------+
| month  | incident_count  |
+--------+-----------------+
| 05     | 11565           |
| 03     | 11266           |
| 06     | 11111           |
| 07     | 10844           |
| 04     | 10732           |
| 08     | 10670           |
| 01     | 10557           |
| 10     | 10365           |
| 09     | 10274           |
| 02     | 9788            |
| 11     | 9365            |
| 12     | 8729            |
+--------+-----------------+
```

# Most Common Borough for "Substantiated" Disposition
```sql
SELECT
    borough,
    COUNT(CASE WHEN dispotion LIKE '%substantiated%' THEN 1 END) AS substantiated_count,
    COUNT(*) AS total_count,
    COUNT(CASE WHEN dispotion LIKE '%substantiated%' THEN 1 END) * 1.0 / COUNT(*) AS substantiated_ratio
FROM complaints
GROUP BY borough
ORDER BY substantiated_ratio DESC;
```
```bash
+----------------+----------------------+--------------+----------------------+
|    borough     | substantiated_count  | total_count  | substantiated_ratio  |
+----------------+----------------------+--------------+----------------------+
| OUTSIDE NYC    | 22                   | 128          | 0.17187500000000000  |
| STATEN ISLAND  | 884                  | 5226         | 0.16915422885572139  |
| BROOKLYN       | 6538                 | 41712        | 0.15674146528576908  |
| MANHATTAN      | 4429                 | 30394        | 0.14571954991116668  |
| BRONX          | 3968                 | 27262        | 0.14555058322940357  |
| QUEENS         | 2884                 | 20093        | 0.14353257353307122  |
| UNKNOWN        | 12                   | 451          | 0.02660753880266075  |
+----------------+----------------------+--------------+----------------------+
```

# Most Common Location Type for "Substantiated" Disposition
```sql
SELECT
    location_type,
    COUNT(CASE WHEN dispotion LIKE '%substantiated%' THEN 1 END) AS substantiated_count,
    COUNT(*) AS total_count,
    COUNT(CASE WHEN dispotion LIKE '%substantiated%' THEN 1 END) * 1.0 / COUNT(*) AS substantiated_ratio
FROM complaints
GROUP BY location_type
ORDER BY substantiated_ratio DESC;
```
```bash
+------------------------+----------------------+--------------+----------------------+
|     location_type      | substantiated_count  | total_count  | substantiated_ratio  |
+------------------------+----------------------+--------------+----------------------+
| Park                   | 229                  | 1415         | 0.16183745583038869  |
| Street/highway         | 10728                | 67351        | 0.15928494008997639  |
| Police building        | 1288                 | 8165         | 0.15774647887323944  |
| School                 | 52                   | 347          | 0.14985590778097983  |
| Apartment/house        | 3242                 | 22116        | 0.14659070356303129  |
| Police vehicle         | 28                   | 192          | 0.14583333333333333  |
| Public space/building  | 334                  | 2299         | 0.14528055676381035  |
| Residential building   | 900                  | 6267         | 0.14360938247965534  |
| River or waterway      | 11                   | 77           | 0.14285714285714286  |
| Hospital               | 103                  | 797          | 0.12923462986198243  |
| Bus                    | 15                   | 120          | 0.12500000000000000  |
| Subway station/train   | 644                  | 5318         | 0.12109815720195562  |
| Commercial building    | 481                  | 4052         | 0.11870681145113524  |
| Other                  | 255                  | 2247         | 0.11348464619492657  |
| NYCHA                  | 161                  | 1632         | 0.09865196078431373  |
| UNKNOWN                | 266                  | 2702         | 0.09844559585492228  |
| Phone                  | 0                    | 169          | 0E-17                |
+------------------------+----------------------+--------------+----------------------+
```

# Most Common Outcome for "Substantiated" Disposition
```sql
SELECT
    outcome,
    COUNT(CASE WHEN dispotion LIKE '%substantiated%' THEN 1 END) AS substantiated_count,
    COUNT(*) AS total_count,
    COUNT(CASE WHEN dispotion LIKE '%substantiated%' THEN 1 END) * 1.0 / COUNT(*) AS substantiated_ratio
FROM complaints
GROUP BY outcome
ORDER BY substantiated_ratio DESC
LIMIT 10;
```
```bash
+---------------------------------------+----------------------+--------------+----------------------+
|                outcome                | substantiated_count  | total_count  | substantiated_ratio  |
+---------------------------------------+----------------------+--------------+----------------------+
| Summons - harrassment (against a PO)  | 6                    | 15           | 0.40000000000000000  |
| Summons - disorderly conduct          | 1134                 | 4459         | 0.25431711145996860  |
| Arrest - disorderly conduct           | 634                  | 2532         | 0.25039494470774092  |
| Arrest - OGA                          | 319                  | 1388         | 0.22982708933717579  |
| Arrest - assault (against a PO)       | 461                  | 2023         | 0.22787938704893722  |
| Juvenile Report                       | 39                   | 185          | 0.21081081081081081  |
| Moving violation summons issued       | 847                  | 4102         | 0.20648464163822526  |
| Arrest - resisting arrest             | 735                  | 3696         | 0.19886363636363636  |
| Other VTL violation summons issued    | 428                  | 2290         | 0.18689956331877729  |
| Arrest - harrassment (against a PO)   | 11                   | 62           | 0.17741935483870968  |
+---------------------------------------+----------------------+--------------+----------------------+
```

# Most Common Month for "Substantiated" Disposition
```sql
SELECT
    SUBSTR(incident_time, 1, 2) AS month,
    COUNT(CASE WHEN dispotion LIKE '%substantiated%' THEN 1 END) AS substantiated_count,
    COUNT(*) AS total_count,
    COUNT(CASE WHEN dispotion LIKE '%substantiated%' THEN 1 END) * 1.0 / COUNT(*) AS substantiated_ratio
FROM complaints
GROUP BY SUBSTR(incident_time, 1, 2)
ORDER BY substantiated_ratio DESC;
```
```bash
+--------+----------------------+--------------+----------------------+
| month  | substantiated_count  | total_count  | substantiated_ratio  |
+--------+----------------------+--------------+----------------------+
| 02     | 1575                 | 9788         | 0.16091131998365345  |
| 03     | 1749                 | 11266        | 0.15524587253683650  |
| 01     | 1593                 | 10557        | 0.15089514066496164  |
| 04     | 1616                 | 10732        | 0.15057771151695863  |
| 10     | 1559                 | 10365        | 0.15041003376748673  |
| 09     | 1544                 | 10274        | 0.15028226591395756  |
| 08     | 1594                 | 10670        | 0.14939081537019681  |
| 11     | 1397                 | 9365         | 0.14917245061398825  |
| 05     | 1699                 | 11565        | 0.14690877648076092  |
| 07     | 1574                 | 10844        | 0.14514939136849871  |
| 12     | 1266                 | 8729         | 0.14503379539466147  |
| 06     | 1571                 | 11111        | 0.14139141391413914  |
+--------+----------------------+--------------+----------------------+
```

# Ratio of "Substantiated" Complaints Grouped by Grouped Outcomes
```sql
SELECT
    CASE
        WHEN LOWER(outcome) LIKE '%arrest%' THEN 'Arrest'
        WHEN LOWER(outcome) LIKE '%summon%' THEN 'Summon'
        ELSE 'Others'
    END AS grouped_outcome,
    COUNT(CASE WHEN LOWER(dispotion) LIKE '%substantiated%' THEN 1 END) AS substantiated_count,
    COUNT(*) AS total_count,
    COUNT(CASE WHEN LOWER(dispotion) LIKE '%substantiated%' THEN 1 END) * 1.0 / COUNT(*) AS substantiated_ratio
FROM complaints
GROUP BY
    CASE
        WHEN LOWER(outcome) LIKE '%arrest%' THEN 'Arrest'
        WHEN LOWER(outcome) LIKE '%summon%' THEN 'Summon'
        ELSE 'Others'
    END
ORDER BY substantiated_ratio DESC;
```
```bash
+------------------+----------------------+--------------+----------------------+
| grouped_outcome  | substantiated_count  | total_count  | substantiated_ratio  |
+------------------+----------------------+--------------+----------------------+
| Summon           | 5499                 | 19813        | 0.27754504618179983  |
| Arrest           | 21226                | 104652       | 0.20282459962542522  |
| Others           | 132                  | 801          | 0.16479400749063670  |
+------------------+----------------------+--------------+----------------------+
```

# Number of Complaints and Ratio of "Substantiated" Complaints Grouped by Year
```sql
SELECT
    SUBSTR(incident_time, 7, 4) AS year,
    COUNT(*) AS complaints,
    COUNT(CASE WHEN LOWER(dispotion) LIKE '%substantiated%' THEN 1 END) * 1.0 / COUNT(*) AS substantiated_ratio
FROM complaints
GROUP BY SUBSTR(incident_time, 7, 4)
ORDER BY substantiated_ratio DESC;
```
```bash
+-------+-------------+----------------------+
| year  | complaints  | substantiated_ratio  |
+-------+-------------+----------------------+
| 2020  | 3618        | 0.29021558872305141  |
| 2021  | 3046        | 0.28562048588312541  |
| 2014  | 4509        | 0.25815036593479707  |
| 2016  | 4117        | 0.25042506679621083  |
| 2013  | 5050        | 0.24415841584158416  |
| 2002  | 4570        | 0.24376367614879650  |
| 2015  | 4287        | 0.24352694191742477  |
| 2017  | 4334        | 0.23834794646977388  |
| 2011  | 5764        | 0.23421235253296322  |
| 2003  | 5453        | 0.22776453328443059  |
| 2018  | 4525        | 0.22121546961325967  |
| 2007  | 7251        | 0.21914218728451248  |
| 2012  | 5470        | 0.21736745886654479  |
| 2001  | 4222        | 0.21009000473709143  |
| 2019  | 4599        | 0.20982822352685366  |
| 2008  | 7041        | 0.20948728873739526  |
| 2022  | 3461        | 0.20832129442357700  |
| 2004  | 6043        | 0.20155551878206189  |
| 2000  | 4009        | 0.19406335744574707  |
| 2005  | 6603        | 0.19218537028623353  |
| 2009  | 7312        | 0.19119256017505470  |
| 2006  | 7398        | 0.18991619356582860  |
| 2010  | 6197        | 0.18928513796998548  |
| 1999  | 214         | 0.16822429906542056  |
| 2023  | 4322        | 0.14229523368810736  |
| 1994  | 2           | 0E-17                |
| 1992  | 1           | 0E-17                |
| 1973  | 1           | 0E-17                |
| 1995  | 1           | 0E-17                |
| 1996  | 2           | 0E-17                |
| 1990  | 1           | 0E-17                |
| 2024  | 1831        | 0E-17                |
+-------+-------------+----------------------+
```

```sql
SELECT
    SUBSTR(incident_time, 7, 4) AS year,
    COUNT(*) AS complaints,
    COUNT(CASE WHEN LOWER(dispotion) LIKE '%substantiated%' THEN 1 END) * 1.0 / COUNT(*) AS substantiated_ratio
FROM complaints
GROUP BY SUBSTR(incident_time, 7, 4)
ORDER BY year;
```
```bash
+-------+-------------+----------------------+
| year  | complaints  | substantiated_ratio  |
+-------+-------------+----------------------+
| 1973  | 1           | 0E-17                |
| 1990  | 1           | 0E-17                |
| 1992  | 1           | 0E-17                |
| 1994  | 2           | 0E-17                |
| 1995  | 1           | 0E-17                |
| 1996  | 2           | 0E-17                |
| 1998  | 12          | 0.58333333333333333  |
| 1999  | 214         | 0.16822429906542056  |
| 2000  | 4009        | 0.19406335744574707  |
| 2001  | 4222        | 0.21009000473709143  |
| 2002  | 4570        | 0.24376367614879650  |
| 2003  | 5453        | 0.22776453328443059  |
| 2004  | 6043        | 0.20155551878206189  |
| 2005  | 6603        | 0.19218537028623353  |
| 2006  | 7398        | 0.18991619356582860  |
| 2007  | 7251        | 0.21914218728451248  |
| 2008  | 7041        | 0.20948728873739526  |
| 2009  | 7312        | 0.19119256017505470  |
| 2010  | 6197        | 0.18928513796998548  |
| 2011  | 5764        | 0.23421235253296322  |
| 2012  | 5470        | 0.21736745886654479  |
| 2013  | 5050        | 0.24415841584158416  |
| 2014  | 4509        | 0.25815036593479707  |
| 2015  | 4287        | 0.24352694191742477  |
| 2016  | 4117        | 0.25042506679621083  |
| 2017  | 4334        | 0.23834794646977388  |
| 2018  | 4525        | 0.22121546961325967  |
| 2019  | 4599        | 0.20982822352685366  |
| 2020  | 3618        | 0.29021558872305141  |
| 2021  | 3046        | 0.28562048588312541  |
| 2022  | 3461        | 0.20832129442357700  |
| 2023  | 4322        | 0.14229523368810736  |
| 2024  | 1831        | 0E-17                |
+-------+-------------+----------------------+
```

# Insights

On streets/highways, the police officers are more likely to be complainted against, with more than 50% of the entries having the location type "street/highway."

Hotter half of the year (May to October) witnesses 6.4% more complaints compared to the cooler half (November to April) (64289 v.s. 60437).

While "OUTSIDE NYC" and "STATEN ISLAND" had fewer complaints than "BROOKLYN," "MANHATTAN," "BRONX," and "QUEENS," higher proportions of their complaints are "substantiated" (where police officers were actually violating the policies).
Other than that, the proportions of complaints being substantiated were positively related to the number of complaints.

While cooler half of the year had fewer complaints, the ratio of substantiated complaints were higher (4 of the top 6 months belong to the cooler half).
