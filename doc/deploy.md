## Deploy a TiKV production cluster

### Deploy a cluster with TiUP

We provide multiple deployment methods, but it is recommended to use our TiUP deployment for production environment.

1. Ensure SSH connectivity between tiup's machine and several target nodes
2. topo.yaml:

```yaml
global:
  user: "ubuntu"
  ssh_port: 22
  deploy_dir: "whatever"
  data_dir: "whatever"

pd_servers:
  - host: x.x.x.x
  - host: y.y.y.y
  - host: z.z.z.z

tikv_servers:
  - host: x.x.x.x
  - host: y.y.y.y
  - host: z.z.z.z

monitoring_servers:
  - host: a.a.a.a

grafana_servers:
  - host: b.b.b.b

alertmanager_servers:
  - host: c.c.c.c
```

3. `$ tiup cluster deploy foobar v4.0.0 ./topo.yaml --user ubuntu -i ~/.ssh/id_rsa`
4. `$ tiup cluster start foobar`
5. You're all set!

