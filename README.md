# NReco.PrestoAdo
Presto/Trino ADO.NET Provider based on [PrestoClient](https://github.com/bamcis-io/PrestoClient). 

* uses Presto API v1
* can connect to Metriql
* compatible with .NET Core / NET5.

Nuget package: [NReco.PrestoAdo](https://www.nuget.org/packages/NReco.PrestoAdo/)

## Connection string

```
Host=hostName;Port=8080;UseSsl=false;Schema=defaultSchema;Catalog=catalog;User=user;Password=password;
```

## Who is using this?
NReco.PrestoAdo is in production use at [SeekTable.com](https://www.seektable.com/) and [PivotData microservice](https://www.nrecosite.com/pivotdata_service.aspx).

## License
Copyright 2021 Vitaliy Fedorchenko

Distributed under the MIT license