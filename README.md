# Dive

A Collection of Fantastic Hive UDFs.

## UDAF
### MaxWhen
**Signature: **`maxwhen(cmp, val)`

**Description**: Returns column `val`'s value from the row where `cmp` has the maximum value. It has an antonym `minwhen`. 

**Example**: For each `uid`, get the latest `ip` .

```sql
select 
    uid, maxwhen(dt, ip) as final_ip
from mydb.mytb
group by uid
```

## License

MIT © Damien Tseng