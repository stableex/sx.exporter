# sx.exporter
SX Exporter 
Prometheus exporter for the stable.sx , stats.sx, swap.sx smart contracts

## Manual Installation

```
pip3 install -r requirements.txt
mkdir -p /usr/lib/systemd/system
cp sx-exporter.service /usr/lib/systemd/system
useradd prometheus
systemctl enable sx-exporter
systemctl start sx-exporter
```

## Usage

```
sx-export.py <options>
  options are:
    -p, --port=<export port> 
    -n, --node=<EOS api endpoint> 
    -r, --refresh=<fetch refresh in seconds>
```
