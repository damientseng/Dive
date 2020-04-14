A collection of UDFs for Hive.

### MAXWHEN/MINWHEN
A UDAF. Use `maxwhen(a, b)` to get column `b`'s value from the row where `a` has a maximum value.

```sql
select
  uid
  ,count(1)        as pv
  ,minwhen(dt, ip) as first_ip
  ,maxwhen(dt, ip) as final_ip
from mydb.mytb
group by uid
```