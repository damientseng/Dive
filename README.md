# Dive
[![Build Status](https://travis-ci.com/damientseng/Dive.svg?branch=master)](https://travis-ci.com/damientseng/Dive)
[![Maintainability](https://api.codeclimate.com/v1/badges/9b6330a7d9ff3a536546/maintainability)](https://codeclimate.com/github/damientseng/Dive/maintainability)

A Collection of Useful Hive UDFs.

## UDAF
### MaxWhen
**Signature**: `maxwhen(cmp, val)`

**Description**: returns column `val`'s value from the row where `cmp` has the maximum value. It has an antonym `minwhen`. 

**Example**: For each `uid`, get the latest `ip` .

```sql
select 
    uid, maxwhen(dt, ip) as final_ip
from mydb.mytb
group by uid
```

### Recent 

**Signature**: `recent(flg, ch)`

**Description**: a user-defined *Analytics* function that combines records without explicit joins. Check out this [post](https://damientseng.com/big-data/2020/04/27/hive-the-udaf-youve-never-seen.html) for more details.
