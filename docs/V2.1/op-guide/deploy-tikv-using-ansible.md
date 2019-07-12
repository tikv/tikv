---
title: Install and Deploy TiKV Using Ansible
summary: Use TiDB-Ansible to deploy a TiKV cluster on multiple nodes.
category: operations
---

# Install and Deploy TiKV Using Ansible

This guide describes how to install and deploy TiKV using Ansible. Ansible is an IT automation tool that can configure systems, deploy software, and orchestrate more advanced IT tasks such as continuous deployments or zero downtime rolling updates.

[TiDB-Ansible](https://github.com/pingcap/tidb-ansible) is a TiDB cluster deployment tool developed by PingCAP, based on Ansible playbook. TiDB-Ansible enables you to quickly deploy a new TiKV cluster which includes PD, TiKV, and the cluster monitoring modules.

> **Warning:** For the production environment, use TiDB-Ansible to deploy your TiKV cluster. If you only want to try TiKV out and explore the features, see [Install and Deploy TiKV using Docker Compose](deploy-tikv-using-docker-compose.md) on a single machine.

## Prepare

Before you start, make sure you have:

1. Several target machines that meet the following requirements:

    - 4 or more machines

        A standard TiKV cluster contains 6 machines. You can use 4 machines for testing.

    - CentOS 7.3 (64 bit) or later with Python 2.7 installed, x86_64 architecture (AMD64)
    - Network between machines
    
    > **Note:** When you deploy TiKV using Ansible, use SSD disks for the data directory of TiKV and PD nodes. Otherwise, the system will not perform well. For more details, see [Software and Hardware Requirements](https://github.com/pingcap/docs/blob/master/op-guide/recommendation.md).

2. A Control Machine that meets the following requirements:

    > **Note:** The Control Machine can be one of the target machines.
    
    - CentOS 7.3 (64 bit) or later with Python 2.7 installed
    - Access to the Internet
    - Git installed

## Step 1: Install system dependencies on the Control Machine

Log in to the Control Machine using the `root` user account, and run the corresponding command according to your operating system.

- If you use a Control Machine installed with CentOS 7, run the following command:

    ```
    # yum -y install epel-release git curl sshpass
    # yum -y install python-pip
    ```

- If you use a Control Machine installed with Ubuntu, run the following command:

    ```
    # apt-get -y install git curl sshpass python-pip
    ```

## Step 2: Create the `tidb` user on the Control Machine and generate the SSH key

Make sure you have logged in to the Control Machine using the `root` user account, and then run the following command.

1. Create the `tidb` user.

    ```
    # useradd -m -d /home/tidb tidb
    ```

2. Set a password for the `tidb` user account.

    ```
    # passwd tidb
    ```

3. Configure sudo without password for the `tidb` user account by adding `tidb ALL=(ALL) NOPASSWD: ALL` to the end of the sudo file:

    ```
    # visudo
    tidb ALL=(ALL) NOPASSWD: ALL
    ```
4. Generate the SSH key.

    Execute the `su` command to switch the user from `root` to `tidb`. Create the SSH key for the `tidb` user account and hit the Enter key when `Enter passphrase` is prompted. After successful execution, the SSH private key file is `/home/tidb/.ssh/id_rsa`, and the SSH public key file is `/home/tidb/.ssh/id_rsa.pub`.

    ```
    # su - tidb
    $ ssh-keygen -t rsa
    Generating public/private rsa key pair.
    Enter file in which to save the key (/home/tidb/.ssh/id_rsa):
    Created directory '/home/tidb/.ssh'.
    Enter passphrase (empty for no passphrase):
    Enter same passphrase again:
    Your identification has been saved in /home/tidb/.ssh/id_rsa.
    Your public key has been saved in /home/tidb/.ssh/id_rsa.pub.
    The key fingerprint is:
    SHA256:eIBykszR1KyECA/h0d7PRKz4fhAeli7IrVphhte7/So tidb@172.16.10.49
    The key's randomart image is:
    +---[RSA 2048]----+
    |=+o+.o.          |
    |o=o+o.oo         |
    | .O.=.=          |
    | . B.B +         |
    |o B * B S        |
    | * + * +         |
    |  o + .          |
    | o  E+ .         |
    |o   ..+o.        |
    +----[SHA256]-----+
    ```

## Step 3: Download TiDB-Ansible to the Control Machine

1. Log in to the Control Machine using the `tidb` user account and enter the `/home/tidb` directory.

2. Download the corresponding TiDB-Ansible version from the [TiDB-Ansible project](https://github.com/pingcap/tidb-ansible). The default folder name is `tidb-ansible`.

    - Download the 2.0 GA version:

        ```bash
        $ git clone -b release-2.0 https://github.com/pingcap/tidb-ansible.git
        ```
    
    - Download the master version:

        ```bash
        $ git clone https://github.com/pingcap/tidb-ansible.git
        ```

    > **Note:** It is required to download `tidb-ansible` to the `/home/tidb` directory using the `tidb` user account. If you download it to the `/root` directory, a privilege issue occurs.

    If you have questions regarding which version to use, email to info@pingcap.com for more information or [file an issue](https://github.com/pingcap/tidb-ansible/issues/new).

## Step 4: Install Ansible and its dependencies on the Control Machine

Make sure you have logged in to the Control Machine using the `tidb` user account.

It is required to use `pip` to install Ansible and its dependencies, otherwise a compatibility issue occurs. Currently, the TiDB 2.0 GA version and the master version are compatible with Ansible 2.4 and Ansible 2.5.

1. Install Ansible and the dependencies on the Control Machine:

    ```bash
    $ cd /home/tidb/tidb-ansible
    $ sudo pip install -r ./requirements.txt
    ```

    Ansible and the related dependencies are in the `tidb-ansible/requirements.txt` file.

2. View the version of Ansible:

    ```bash
    $ ansible --version
    ansible 2.5.0
    ```

## Step 5: Configure the SSH mutual trust and sudo rules on the Control Machine

Make sure you have logged in to the Control Machine using the `tidb` user account.

1. Add the IPs of your target machines to the `[servers]` section of the `hosts.ini` file.

    ```bash
    $ cd /home/tidb/tidb-ansible
    $ vi hosts.ini
    [servers]
    172.16.10.1
    172.16.10.2
    172.16.10.3
    172.16.10.4
    172.16.10.5
    172.16.10.6

    [all:vars]
    username = tidb
    ntp_server = pool.ntp.org
    ```

2. Run the following command and input the `root` user account password of your target machines.

    ```bash
    $ ansible-playbook -i hosts.ini create_users.yml -u root -k
    ```

    This step creates the `tidb` user account on the target machines, and configures the sudo rules and the SSH mutual trust between the Control Machine and the target machines.

> **Note:** To configure the SSH mutual trust and sudo without password manually, see [How to manually configure the SSH mutual trust and sudo without password](https://github.com/pingcap/docs/blob/master/op-guide/ansible-deployment.md#how-to-manually-configure-the-ssh-mutual-trust-and-sudo-without-password).

## Step 6: Install the NTP service on the target machines

> **Note:** If the time and time zone of all your target machines are same, the NTP service is on and is normally synchronizing time, you can ignore this step. See [How to check whether the NTP service is normal](https://github.com/pingcap/docs/blob/master/op-guide/ansible-deployment.md#how-to-check-whether-the-ntp-service-is-normal).

Make sure you have logged in to the Control Machine using the `tidb` user account, run the following command:

```bash
$ cd /home/tidb/tidb-ansible
$ ansible-playbook -i hosts.ini deploy_ntp.yml -u tidb -b
```

The NTP service is installed and started using the software repository that comes with the system on the target machines. The default NTP server list in the installation package is used. The related `server` parameter is in the `/etc/ntp.conf` configuration file.

To make the NTP service start synchronizing as soon as possible, the system executes the `ntpdate` command to set the local date and time by polling `ntp_server` in the `hosts.ini` file. The default server is `pool.ntp.org`, and you can also replace it with your NTP server.

## Step 7: Configure the CPUfreq governor mode on the target machine

For details about CPUfreq, see [the CPUfreq Governor documentation](https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/7/html/power_management_guide/cpufreq_governors).

Set the CPUfreq governor mode to `performance` to make full use of CPU performance.

### Check the governor modes supported by the system

You can run the `cpupower frequency-info --governors` command to check the governor modes which the system supports:

```
# cpupower frequency-info --governors
analyzing CPU 0:
  available cpufreq governors: performance powersave
```

Taking the above code for example, the system supports the `performance` and `powersave` modes. 

> **Note:** As the following shows, if it returns "Not Available", it means that the current system does not support CPUfreq configuration and you can skip this step.

```
# cpupower frequency-info --governors
analyzing CPU 0:
   available cpufreq governors: Not Available
```

### Check the current governor mode

You can run the `cpupower frequency-info --policy` command to check the current CPUfreq governor mode:

```
# cpupower frequency-info --policy
analyzing CPU 0:
  current policy: frequency should be within 1.20 GHz and 3.20 GHz.
                  The governor "powersave" may decide which speed to use
                  within this range.
```

As the above code shows, the current mode is `powersave` in this example.

### Change the governor mode

- You can run the following command to change the current mode to `performance`:

    ```
    # cpupower frequency-set --governor performance
    ```

- You can also run the following command to set the mode on the target machine in batches:

    ```
    $ ansible -i hosts.ini all -m shell -a "cpupower frequency-set --governor performance" -u tidb -b
    ```

## Step 8: Mount the data disk ext4 filesystem with options on the target machines

Log in to the Control Machine using the `root` user account.

Format your data disks to the ext4 filesystem and mount the filesystem with the `nodelalloc` and `noatime` options. It is required to mount the `nodelalloc` option, or else the Ansible deployment cannot pass the test. The `noatime` option is optional.

> **Note:** If your data disks have been formatted to ext4 and have mounted the options, you can uninstall it by running the `# umount /dev/nvme0n1` command, follow the steps starting from editing the `/etc/fstab` file, and remount the filesystem with options.

Take the `/dev/nvme0n1` data disk as an example:

1. View the data disk.

    ```
    # fdisk -l
    Disk /dev/nvme0n1: 1000 GB
    ```

2. Create the partition table.

    ```
    # parted -s -a optimal /dev/nvme0n1 mklabel gpt -- mkpart primary ext4 1 -1
    ```

3. Format the data disk to the ext4 filesystem.

    ```
    # mkfs.ext4 /dev/nvme0n1
    ```

4. View the partition UUID of the data disk.

    In this example, the UUID of `nvme0n1` is `c51eb23b-195c-4061-92a9-3fad812cc12f`.

    ```
    # lsblk -f
    NAME    FSTYPE LABEL UUID                                 MOUNTPOINT
    sda
    ├─sda1  ext4         237b634b-a565-477b-8371-6dff0c41f5ab /boot
    ├─sda2  swap         f414c5c0-f823-4bb1-8fdf-e531173a72ed
    └─sda3  ext4         547909c1-398d-4696-94c6-03e43e317b60 /
    sr0
    nvme0n1 ext4         c51eb23b-195c-4061-92a9-3fad812cc12f
    ```

5. Edit the `/etc/fstab` file and add the mount options.

    ```
    # vi /etc/fstab
    UUID=c51eb23b-195c-4061-92a9-3fad812cc12f /data1 ext4 defaults,nodelalloc,noatime 0 2
    ```

6. Mount the data disk.

    ```
    # mkdir /data1
    # mount -a
    ```

7. Check using the following command.

    ```
    # mount -t ext4
    /dev/nvme0n1 on /data1 type ext4 (rw,noatime,nodelalloc,data=ordered)
    ```

    If the filesystem is ext4 and `nodelalloc` is included in the mount options, you have successfully mount the data disk ext4 filesystem with options on the target machines.

## Step 9: Edit the `inventory.ini` file to orchestrate the TiKV cluster

Edit the `tidb-ansible/inventory.ini` file to orchestrate the TiKV cluster. The standard TiKV cluster contains 6 machines: 3 PD nodes and 3 TiKV nodes.

- Deploy at least 3 instances for TiKV.
- Do not deploy TiKV together with PD on the same machine.
- Use the first PD machine as the monitoring machine.

> **Note:**
>
> - Leave `[tidb_servers]` in the `inventory.ini` file empty, because this deployment is for the TiKV cluster, not the TiDB cluster.
> - It is required to use the internal IP address to deploy. If the SSH port of the target machines is not the default 22 port, you need to add the `ansible_port` variable. For example, `TiDB1 ansible_host=172.16.10.1 ansible_port=5555`.

You can choose one of the following two types of cluster topology according to your scenario:

- [The cluster topology of a single TiKV instance on each TiKV node](#option-1-use-the-cluster-topology-of-a-single-tikv-instance-on-each-tikv-node)

    In most cases, it is recommended to deploy one TiKV instance on each TiKV node for better performance. However, if the CPU and memory of your TiKV machines are much better than the required in [Hardware and Software Requirements](https://github.com/pingcap/docs/blob/master/op-guide/recommendation.md), and you have more than two disks in one node or the capacity of one SSD is larger than 2 TB, you can deploy no more than 2 TiKV instances on a single TiKV node.

- [The cluster topology of multiple TiKV instances on each TiKV node](#option-2-use-the-cluster-topology-of-multiple-tikv-instances-on-each-tikv-node)

### Option 1: Use the cluster topology of a single TiKV instance on each TiKV node

| Name  | Host IP     | Services |
|-------|-------------|----------|
| node1 | 172.16.10.1 | PD1      |
| node2 | 172.16.10.2 | PD2      |
| node3 | 172.16.10.3 | PD3      |
| node4 | 172.16.10.4 | TiKV1    |
| node5 | 172.16.10.5 | TiKV2    |
| node6 | 172.16.10.6 | TiKV3    |

Edit the `inventory.ini` file as follows:

```ini
[tidb_servers]

[pd_servers]
172.16.10.1
172.16.10.2
172.16.10.3

[tikv_servers]
172.16.10.4
172.16.10.5
172.16.10.6

[monitoring_servers]
172.16.10.1

[grafana_servers]
172.16.10.1

[monitored_servers]
172.16.10.1
172.16.10.2
172.16.10.3
172.16.10.4
172.16.10.5
172.16.10.6
```

### Option 2: Use the cluster topology of multiple TiKV instances on each TiKV node

Take two TiKV instances on each TiKV node as an example:

| Name  | Host IP     | Services         |
|-------|-------------|------------------|
| node1 | 172.16.10.1 | PD1              |
| node2 | 172.16.10.2 | PD2              |
| node3 | 172.16.10.3 | PD3              |
| node4 | 172.16.10.4 | TiKV1-1, TiKV1-2 |
| node5 | 172.16.10.5 | TiKV2-1, TiKV2-2 |
| node6 | 172.16.10.6 | TiKV3-1, TiKV3-2 |

```ini
[tidb_servers]

[pd_servers]
172.16.10.1
172.16.10.2
172.16.10.3

[tikv_servers]
TiKV1-1 ansible_host=172.16.10.4 deploy_dir=/data1/deploy tikv_port=20171 labels="host=tikv1"
TiKV1-2 ansible_host=172.16.10.4 deploy_dir=/data2/deploy tikv_port=20172 labels="host=tikv1"
TiKV2-1 ansible_host=172.16.10.5 deploy_dir=/data1/deploy tikv_port=20171 labels="host=tikv2"
TiKV2-2 ansible_host=172.16.10.5 deploy_dir=/data2/deploy tikv_port=20172 labels="host=tikv2"
TiKV3-1 ansible_host=172.16.10.6 deploy_dir=/data1/deploy tikv_port=20171 labels="host=tikv3"
TiKV3-2 ansible_host=172.16.10.6 deploy_dir=/data2/deploy tikv_port=20172 labels="host=tikv3"

[monitoring_servers]
172.16.10.1

[grafana_servers]
172.16.10.1

[monitored_servers]
172.16.10.1
172.16.10.2
172.16.10.3
172.16.10.4
172.16.10.5
172.16.10.6

...

[pd_servers:vars]
location_labels = ["host"]
```

Edit the parameters in the service configuration file:

1. For the cluster topology of multiple TiKV instances on each TiKV node, you need to edit the `block-cache-size` parameter in `tidb-ansible/conf/tikv.yml`:

    - `rocksdb defaultcf block-cache-size(GB)`: MEM * 80% / TiKV instance number * 30%
    - `rocksdb writecf block-cache-size(GB)`: MEM * 80% / TiKV instance number * 45%
    - `rocksdb lockcf block-cache-size(GB)`: MEM * 80% / TiKV instance number * 2.5% (128 MB at a minimum)
    - `raftdb defaultcf block-cache-size(GB)`: MEM * 80% / TiKV instance number * 2.5% (128 MB at a minimum)

2. For the cluster topology of multiple TiKV instances on each TiKV node, you need to edit the `high-concurrency`, `normal-concurrency` and `low-concurrency` parameters in the `tidb-ansible/conf/tikv.yml` file:

    ```
    readpool:
    coprocessor:
        # Notice: if CPU_NUM > 8, default thread pool size for coprocessors
        # will be set to CPU_NUM * 0.8.
        # high-concurrency: 8
        # normal-concurrency: 8
        # low-concurrency: 8
    ```

    Recommended configuration: `number of instances * parameter value = CPU_Vcores * 0.8`.

3. If multiple TiKV instances are deployed on a same physical disk, edit the `capacity` parameter in `conf/tikv.yml`:

    - `capacity`: total disk capacity / number of TiKV instances (the unit is GB)

## Step 10: Edit variables in the `inventory.ini` file

1. Edit the `deploy_dir` variable to configure the deployment directory.

    The global variable is set to `/home/tidb/deploy` by default, and it applies to all services. If the data disk is mounted on the `/data1` directory, you can set it to `/data1/deploy`. For example:

    ```bash
    ## Global variables
    [all:vars]
    deploy_dir = /data1/deploy
    ```

    **Note:** To separately set the deployment directory for a service, you can configure the host variable while configuring the service host list in the `inventory.ini` file. It is required to add the first column alias, to avoid confusion in scenarios of mixed services deployment.

    ```bash
    TiKV1-1 ansible_host=172.16.10.4 deploy_dir=/data1/deploy
    ```

2. Set the `deploy_without_tidb` variable to `True`.

    ```bash
    deploy_without_tidb = True
    ```

> **Note:** If you need to edit other variables, see [the variable description table](https://github.com/pingcap/docs/blob/master/op-guide/ansible-deployment.md#edit-other-variables-optional).

## Step 11: Deploy the TiKV cluster

When `ansible-playbook` executes the Playbook, the default concurrent number is 5. If many target machines are deployed, you can add the `-f` parameter to specify the concurrency, such as `ansible-playbook deploy.yml -f 10`.

The following example uses `tidb` as the user who runs the service.

1. Check the `tidb-ansible/inventory.ini` file to make sure `ansible_user = tidb`.

    ```bash
    ## Connection
    # ssh via normal user
    ansible_user = tidb
    ```

2. Make sure the SSH mutual trust and sudo without password are successfully configured.

    - Run the following command and if all servers return `tidb`, then the SSH mutual trust is successfully configured:

        ```bash
        ansible -i inventory.ini all -m shell -a 'whoami'
        ```

    - Run the following command and if all servers return `root`, then sudo without password of the `tidb` user is successfully configured:

        ```bash
        ansible -i inventory.ini all -m shell -a 'whoami' -b
        ```

3. Download the TiKV binary to the Control Machine.

    ```bash
    ansible-playbook local_prepare.yml
    ```

4. Initialize the system environment and modify the kernel parameters.

    ```bash
    ansible-playbook bootstrap.yml
    ```

5. Deploy the TiKV cluster.

    ```bash
    ansible-playbook deploy.yml
    ```

6. Start the TiKV cluster.

    ```bash
    ansible-playbook start.yml
    ```

You can check whether the TiKV cluster has been successfully deployed using the following command:

```bash
curl 172.16.10.1:2379/pd/api/v1/stores
```

If you want to try the Go client, see [Try Two Types of APIs](../clients/go-client-api.md).

## Stop the TiKV cluster

If you want to stop the TiKV cluster, run the following command:

```bash
ansible-playbook stop.yml
```

## Destroy the TiKV cluster

> **Warning:** Before you clean the cluster data or destroy the TiKV cluster, make sure you do not need it any more.

- If you do not need the data any more, you can clean up the data for test using the following command:

    ```
    ansible-playbook unsafe_cleanup_data.yml
    ```

- If you do not need the TiKV cluster any more, you can destroy it using the following command:

    ```bash
    ansible-playbook unsafe_cleanup.yml
    ```
    
    > **Note:** If the deployment directory is a mount point, an error might be reported, but the implementation result remains unaffected. You can just ignore the error.