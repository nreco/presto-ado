# NReco.PrestoAdo
Presto/Trino ADO.NET Provider based on [PrestoClient](https://github.com/bamcis-io/PrestoClient). 

* uses Presto/Trino API v1 (headers prefix can be switched with "TrinoHeaders=1" in a connection string)
* can connect to Metriql
* compatible with .NET Core / NET6.

Nuget package: [NReco.PrestoAdo](https://www.nuget.org/packages/NReco.PrestoAdo/)

## Connection string

```
Host=hostName;Port=8080;UseSsl=false;Schema=defaultSchema;Catalog=catalog;User=user;Password=password;TrinoHeaders=1;
```

## Who is using this?
NReco.PrestoAdo is in production use at [SeekTable.com](https://www.seektable.com/) and [PivotData microservice](https://www.nrecosite.com/pivotdata_service.aspx).

## License
Copyright 2021-2022 Vitaliy Fedorchenko

Distributed under the MIT license